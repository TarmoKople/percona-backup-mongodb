FROM golang:bullseye
RUN apt-get update -y && apt-get install -y libkrb5-dev
WORKDIR /opt/pbm
COPY . .
RUN go build ./cmd/pbm-rs/pbm-rs.go

FROM mongo:5

COPY --from=0 /opt/pbm/pbm-rs /usr/local/bin/

USER nobody

CMD ["/usr/local/bin/pbm-rs", "restore", "$(PBM_BACKUP_NAME)", "$(PBM_RS_NAME)"]
