# ifremer-sync

This repo contains the scripts and documentation needed to re-sync the entire Argo dataset from ifremer, and re-establish regular imports of this data to Argovis.

## Rebuilding from scratch

If you have no files downloaded from ifremer and nothing in your `argo` or `argoMeta` collections (but have defined those collections per [https://github.com/argovis/db-schema](https://github.com/argovis/db-schema)) (ie you are rebuilding from nothing):

 - Start by rsyncing ifremer's argo data: `rsync -avzhi --delete --omit-dir-times --no-perms vdmzrs.ifremer.fr::argo/ /ifremer`.
 - Follow the instruction in the 'Rebuild mongo argo collections without repeating rsync' section to load these results in to MongoDB
 - Build the image defined in `Dockerfile`, and run it as the image in the Kube cron job described in `ifremer-cron.yaml` if you're orchestrating with Kube, or as a regular cronjob via `ifremer-cron.sh` on Swarm or a bare container server. Note the storage requirements assumed in both cases.

Note the first two steps together can take _weeks_, depending on resourcing. From there, if all goes well, your cron script of choice will update your MongoDB instance with new data nightly. Check the logs periodically, as edge cases do appear in the Argo data, and decisions may have to be made on how you'd like your Argovis instance to handle them.

## Rebuild mongo argo collections without repeating rsync

If for some reason the rsync'ed mirror is intact but the mongo collections need to be rebuilt from scratch (irrecoverably corrupted or a schema change), see `freshrebuild.sh`. This assumes the rsync mirror can be found in the filesystem at `/ifremer` (ie mount `ifremer-mirror` PVC at `/ifremer`), and will leave you with a file `/tmp/profiles2translate` appropriate for feeding to `testload.sh` to rebuild the `argo` and `argoMeta` collections. This workflow was confirmed to produce _exactly_ the same amount of lines in `/tmp/profiles2translate` as there were argo profiles concurrently in mongo, as it should; future runs should verify this where possible.

Consider parallelizing this by slicing `/tmp/profiles2translate` into equal parts and running one per pod, for example as in `devpod.yaml`.

Note also `testload.sh` has some simple fault tolerance built in, and will try to keep track of progress and resume after an interrupt; checking profiles immediately before and after these breakpoints is the first place to look if an unexpected(ly small) number of profiles appear in the final collection rebuild.

## Manually redo a failed nightly update

If something interrupts a nightly update that finished rsync'ing and parsing the rsync log but was interrupted during the mongo load, best to suspend the cronjob and redo that evening's update; see `redo-sync.yaml` for a pod to manage `ifremer-sync-redo.sh`, which takes the existing `updatedprofiles` list in the logging directory you must specify in the yaml file's command, and reruns the corresponding uploads to mongo.

## Integrity checking

`roundtrip.[py|yaml]` and `Dockerfile-roundtrip` define a pod that will randomly pick profiles from mongo, redownload the ifremer source that defines them, and double checks the collection contents are correct. This is meant to run as a background process to flag errors and demonstrate robustness.