# per http://www.argodatamgt.org/Access-to-data/Argo-GDAC-synchronization-service
rsync -avzhi --delete --omit-dir-times --no-perms vdmzrs.ifremer.fr::argo/ /ifremer > /logs/synclog