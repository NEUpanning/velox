set -e

if [ $# -ne 1 ]; then
    echo "no dst path!!!"
    exit 1
fi

SCRIPTDIR=$(dirname "$0")
FROM_PATH=${SCRIPTDIR}
DST_PATH=$1

cp -v ${FROM_PATH}/lib/* ${DST_PATH}/

cd $DST_PATH
ln -sf libhdfs3.so.2.2.30 libhdfs3.so.1
cd -
