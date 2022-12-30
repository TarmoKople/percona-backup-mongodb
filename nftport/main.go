package nftport

import (
	"fmt"
	"github.com/alecthomas/kingpin"
	"github.com/docker/go-units"
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"strings"
	"time"
)

func Main() {

	var (
		app    = kingpin.New("pbm-rs", "PBM shard restore")
		cfg    pbm.Config
		region = app.Flag("Region", "S3 region env:PBM_REGION").Short('r').Default("eu-west-1").Envar("PBM_REGION").String()
		bucket = app.Flag("bucket", "S3 bucket env:PBM_BUCKET").Short('b').Default("mongobackup.nftport.eu-west").Envar("PBM_BUCKET").String()
		prefix = app.Flag("prefix", "Backup directory prefix (usually <k8s cluster>/<MongoDB cluster>) env:PBM_PREFIX").Short('p').Envar("PBM_PREFIX").String()
	)

	app.HelpFlag.Short('h')

	listCmd := app.Command("list", "List backups from storage")
	describeCmd := app.Command("describe", "Describe backup")
	describeBackupName := describeCmd.Arg("backupName", "Backup name env:PBM_BACKUP_NAME").Required().Envar("PBM_BACKUP_NAME").String()
	restoreCmd := app.Command("restore", "Restore replicaset member")
	restoreBackupName := restoreCmd.Arg("backupName", "Backup name env:PBM_BACKUP_NAME").Required().Envar("PBM_BACKUP_NAME").String()
	restoreRsName := restoreCmd.Arg("rsName", "RS name to restore env:PBM_RS_NAME").Required().Envar("PBM_RS_NAME").String()
	restoreDbPath := restoreCmd.Flag("dbpath", "Data directory (mongod --dbpath ...) env:PBM_DB_PATH").Default("/data/db").Envar("PBM_DB_PATH").String()

	cmd, err := app.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: parse command line parameters:", err)
		os.Exit(1)
	}

	cfg.Storage.Type = storage.S3
	cfg.Storage.S3.Bucket = *bucket
	cfg.Storage.S3.Region = *region
	cfg.Storage.S3.Prefix = *prefix

	switch cmd {
	case listCmd.FullCommand():
		listBackups(cfg)
	case describeCmd.FullCommand():
		//if *describeBackupName == "" {
		//	fmt.Fprintf(os.Stderr, "Error, backupName not set")
		//}
		describeBackup(cfg, *describeBackupName)
	case restoreCmd.FullCommand():
		RestoreRS(cfg, *restoreBackupName, *restoreRsName, *restoreDbPath)
		fmt.Println("Done !")
		for {
			time.Sleep(1 * time.Second)
		}
	}
}

func listBackups(cfg pbm.Config) {
	st, err := pbm.Storage(cfg, log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Storage error: %v", err)
		os.Exit(1)
	}

	backupList, err := st.List("", ".pbm.json")

	if err != nil {
		fmt.Fprintf(os.Stderr, "Backup list error: %v", err)
		os.Exit(2)
	}

	numBackups := 0
	for _, b := range backupList {
		if strings.Contains(b.Name, "/") {
			continue
		}
		numBackups++
		backupName := strings.TrimSuffix(b.Name, ".pbm.json")
		bcp, err := restore.GetMetaFromStore(st, backupName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can not read backup metadata for backup %s, error: %s", backupName, err.Error())
		}

		fmt.Printf("%s\t%s\t%s\t%s\n", backupName, bcp.Type, bcp.Compression, units.BytesSize(float64(bcp.Size)))

	}
	if numBackups == 0 {
		fmt.Println("No backups found, please check S3 bucket / prefix settings")
	}
}

func describeBackup(cfg pbm.Config, backupName string) {
	st, err := pbm.Storage(cfg, log.New(nil, "cli", "").NewEvent("", "", "", primitive.Timestamp{}))
	bcp, err := restore.GetMetaFromStore(st, backupName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can not read backup metadata for backup %s, error: %s", backupName, err.Error())
	}
	fmt.Printf("Name:\t%s\n", bcp.Name)
	fmt.Printf("Type:\t%s\n", bcp.Type)
	fmt.Printf("Compr:\t%s\n", bcp.Compression)
	fmt.Printf("Size:\t%s\n", units.BytesSize(float64(bcp.Size)))
	fmt.Printf("Opid:\t%s\n", bcp.OPID)
	fmt.Printf("Repl sets:\t%d\n", len(bcp.Replsets))
	for _, s := range bcp.Replsets {
		fmt.Printf("rsName: %s, num files %d: \n", s.Name, len(s.Files))
	}

}
