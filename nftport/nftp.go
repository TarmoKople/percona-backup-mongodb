package nftport

import (
	"bytes"
	"context"
	"fmt"
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	slog "log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	mongofslock       = "mongod.lock"
	internalMongodLog = "pbm.restore.log"
)

var (
	dbpath string
)

func RestoreRS(cfg pbm.Config, backupName string, rsName string, d string) {
	dbpath = d

	stg, err := pbm.Storage(cfg, log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{}))

	if err != nil {
		fmt.Println(err.Error())
	}

	bcp, err := restore.GetMetaFromStore(stg, backupName)

	err = copyFiles(bcp, rsName, stg)
	if err != nil {
		fmt.Println("copyFiles error:" + err.Error())
	}
	//
	err = prepareData(bcp)
	if err != nil {
		fmt.Println("prepareData error: " + err.Error())
	}

	err = recoverStandalone()
	if err != nil {
		fmt.Println("recoverStandalone error: " + err.Error())
	}

	err = resetRS(bcp, rsName)
	if err != nil {
		fmt.Println("resetRS error: " + err.Error())
	}
}

func copyFiles(bcp *pbm.BackupMeta, rsName string, stg storage.Storage) error {
	err := RemoveContents(dbpath)
	if err != nil {
		os.Exit(1)
	}
	for _, rs := range bcp.Replsets {
		if rs.Name == rsName {
			for _, f := range rs.Files {
				src := filepath.Join(bcp.Name, rsName, f.Name+bcp.Compression.Suffix())
				dst := filepath.Join(dbpath, f.Name)

				err := os.MkdirAll(filepath.Dir(dst), os.ModeDir|0o700)
				if err != nil {
					return errors.Wrapf(err, "create path %s", filepath.Dir(dst))
				}

				//r.log.Info("copy <%s> to <%s>", src, dst)
				fmt.Printf("copy <%s> to <%s>\n", src, dst)
				sr, err := stg.SourceReader(src)
				if err != nil {
					return errors.Wrapf(err, "create source reader for <%s>", src)
				}
				defer sr.Close()

				data, err := compress.Decompress(sr, bcp.Compression)
				if err != nil {
					return errors.Wrapf(err, "decompress object %s", src)
				}
				defer data.Close()

				fw, err := os.Create(dst)
				if err != nil {
					return errors.Wrapf(err, "create destination file <%s>", dst)
				}
				defer fw.Close()
				err = os.Chmod(dst, f.Fmode)
				if err != nil {
					return errors.Wrapf(err, "change permissions for file <%s>", dst)
				}

				_, err = io.Copy(fw, data)
				if err != nil {
					return errors.Wrapf(err, "copy file <%s>", dst)
				}
			}
		}
	}
	return nil
}

func prepareData(bcp *pbm.BackupMeta) error {
	port, err := peekTmpPort()

	err = startMongo("--dbpath", dbpath, "--port", strconv.Itoa(port),
		"--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(port, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	ctx := context.Background()

	//err = c.Database("local").Collection("replset.minvalid").Drop(ctx)
	//if err != nil {
	//	return errors.Wrap(err, "drop replset.minvalid")
	//}
	//err = c.Database("local").Collection("replset.oplogTruncateAfterPoint").Drop(ctx)
	//if err != nil {
	//	return errors.Wrap(err, "drop replset.oplogTruncateAfterPoint")
	//}
	//err = c.Database("local").Collection("replset.election").Drop(ctx)
	//if err != nil {
	//	return errors.Wrap(err, "drop replset.election")
	//}
	//_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	//if err != nil {
	//	return errors.Wrap(err, "delete from system.replset")
	//}

	_, err = c.Database("local").Collection("replset.minvalid").InsertOne(ctx,
		bson.M{"_id": primitive.NewObjectID(), "t": -1, "ts": primitive.Timestamp{0, 1}},
	)
	if err != nil {
		return errors.Wrap(err, "insert to replset.minvalid")
	}

	fmt.Printf("oplogTruncateAfterPoint: %v", bcp.LastWriteTS)
	_, err = c.Database("local").Collection("replset.oplogTruncateAfterPoint").InsertOne(ctx,
		bson.M{"_id": "oplogTruncateAfterPoint", "oplogTruncateAfterPoint": bcp.LastWriteTS},
	)
	if err != nil {
		return errors.Wrap(err, "set oplogTruncateAfterPoint")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

// peeks a random free port in a range [minPort, maxPort]
func peekTmpPort() (int, error) {
	current := 27017
	const (
		rng = 1111
		try = 150
	)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < try; i++ {
		p := current + rand.Intn(rng) + 1
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(p))
		if err == nil {
			ln.Close()
			return p, nil
		}
	}

	return -1, errors.Errorf("can't find unused port in range [%d, %d]", current, current+rng)
}

func conn(port int, rs string) (*mongo.Client, error) {
	ctx := context.Background()

	opts := options.Client().
		SetHosts([]string{"localhost:" + strconv.Itoa(port)}).
		SetAppName("pbm-physical-restore").
		SetDirect(true).
		SetConnectTimeout(time.Second * 60)

	if rs != "" {
		opts = opts.SetReplicaSet(rs)
	}

	conn, err := mongo.NewClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}

	err = conn.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

func startMongo(opts ...string) error {
	//if r.tmpConf != nil {
	//	opts = append(opts, []string{"-f", r.tmpConf.Name()}...)
	//}

	opts = append(opts, []string{"--logpath", path.Join(dbpath, internalMongodLog)}...)

	errBuf := new(bytes.Buffer)
	cmd := exec.Command("mongod", opts...)

	cmd.Stderr = errBuf
	err := cmd.Start()
	if err != nil {
		return err
	}

	// release process resources
	go func() {
		err := cmd.Wait()
		if err != nil {
			slog.Printf("mongod process: %v, %s", err, errBuf)
		}
	}()
	return nil
}

func shutdown(c *mongo.Client) error {
	err := c.Database("admin").RunCommand(context.Background(), bson.D{{"shutdown", 1}}).Err()
	if err != nil && !strings.Contains(err.Error(), "socket was unexpectedly closed") {
		return err
	}
	err = waitMgoShutdown(dbpath)
	if err != nil {
		return errors.Wrap(err, "shutdown")
	}
	return nil
}

func waitMgoShutdown(dbpath string) error {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for range tk.C {
		f, err := os.Stat(path.Join(dbpath, mongofslock))
		if err != nil {
			return errors.Wrapf(err, "check for lock file %s", path.Join(dbpath, mongofslock))
		}

		if f.Size() == 0 {
			return nil
		}
	}

	return nil
}

func recoverStandalone() error {
	tmpPort, err := peekTmpPort()
	err = startMongo("--dbpath", dbpath, "--port", strconv.Itoa(tmpPort),
		"--setParameter", "recoverFromOplogAsStandalone=true",
		"--setParameter", "takeUnstableCheckpointOnShutdown=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(tmpPort, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func resetRS(bcp *pbm.BackupMeta, rs string) error {
	tmpPort, err := peekTmpPort()
	err = startMongo("--dbpath", dbpath, "--port", strconv.Itoa(tmpPort),
		"--setParameter", "disableLogicalSessionCacheRefresh=true",
		"--setParameter", "skipShardingConfigurationChecks=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(tmpPort, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	ctx := context.Background()

	err = c.Database("config").Collection("mongos").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.mongos")
	}
	err = c.Database("config").Collection("lockpings").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.lockpings")
	}

	err = c.Database("config").Collection("cache.collections").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.collections")
	}

	err = c.Database("config").Collection("cache.databases").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.databases")
	}

	err = c.Database("config").Collection("cache.chunks.config.system.sessions").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.chunks.config.system.sessions")
	}

	const retry = 5
	for i := 0; i < retry; i++ {
		err = c.Database("config").Collection("system.sessions").Drop(ctx)
		if err == nil || !strings.Contains(err.Error(), "(BackgroundOperationInProgressForNamespace)") {
			break
		}
		fmt.Println("drop config.system.sessions: BackgroundOperationInProgressForNamespace, retrying")
		time.Sleep(time.Second * time.Duration(i+1))
	}
	if err != nil {
		return errors.Wrap(err, "drop config.system.sessions")
	}

	_, err = c.Database("admin").Collection("system.users").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "drop config.system.users")
	}
	_, err = c.Database("admin").Collection("system.roles").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "drop config.system.roles")
	}

	_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete from system.replset")
	}

	//_, err = c.Database("local").Collection("system.replset").InsertOne(ctx,
	//	pbm.RSConfig{
	//		ID:       r.rsConf.ID,
	//		CSRS:     r.nodeInfo.IsConfigSrv(),
	//		Version:  1,
	//		Members:  r.rsConf.Members,
	//		Settings: r.rsConf.Settings,
	//	},
	//)
	//if err != nil {
	//	return errors.Wrapf(err, "upate rs.member host to %s", r.nodeInfo.Me)
	//}

	// PITR should be turned off after the physical restore. Otherwise, slicing resumes
	// right after the cluster start while the system in the state of the backup's
	// recovery time. No resync yet. Hence the system knows nothing about the recent
	// restore and chunks made after the backup. So it would successfully start slicing
	// and overwrites chunks after the backup.
	//if r.nodeInfo.IsLeader() {
	_, err = c.Database(pbm.DB).Collection(pbm.ConfigCollection).UpdateOne(ctx, bson.D{},
		bson.D{{"$set", bson.M{"pitr.enabled": false}}},
	)
	if err != nil {
		return errors.Wrap(err, "turn off pitr")
	}
	//}

	//err := c.Database("admin").

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		// don't return error, if dir is not existing
		return nil
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
