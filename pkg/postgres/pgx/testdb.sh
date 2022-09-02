#!/bin/sh

ITEST_SQL2KEYVAL_PGX_DBNAME=itest_sql2keyval_pgx
img=postgres:14.5-alpine3.16
name=$ITEST_SQL2KEYVAL_PGX_DBNAME

sockdir=$PWD/test.d/sock.d
#datadir=~/path/2/test/dir

mkdir -p $sockdir
#mkdir -p $datadir

docker rm --force $name

docker \
  run \
  --name $name \
  --detach \
  --env POSTGRES_PASSWORD=postgres \
  --env PGDATA=/pgdata/data \
  --env TZ=Etc/UTC \
  --volume $sockdir:/var/run/postgresql \
  $img
  #--volume $datadir:/pgdata/data \

export PGUSER=postgres
export PGHOST=$sockdir

echo waiting db ready...
while ( pg_isready --timeout 60 1>/dev/null 2>/dev/null && echo ok || echo ng ) | fgrep -q ng; do
	sleep 1
done

echo "CREATE DATABASE $ITEST_SQL2KEYVAL_PGX_DBNAME" | PGUSER=postgres PGHOST=$sockdir psql
