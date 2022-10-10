# image argovis/ifremer-sync:nightly built from Dockerfile in this repo
# run with crontab 0 7 * * *

if test -f "/home/ubuntu/ifremerlock"; then
    exit 1
fi

touch /home/ubuntu/ifremerlock
docker container run --rm  --network argovis-db --name ifremer-nightly -v /home/ubuntu/ifremer:/ifremer -v /home/ubuntu/logs:/logs -d argovis/ifremer-sync:nightly bash ifremer-sync.sh
rm /home/ubuntu/ifremerlock