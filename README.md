# ifremer-sync

This repo contains the scripts and documentation needed to re-sync the entire Argo dataset from ifremer, and re-establish regular imports of this data to Argovis.

## Usage

 - Required volumes:
   - `ifremer-mirror`: contains the files synced from ifremer.
   - `logs`: scratch space for storing logs.
 - Set up as a cronjob in kube using `ifremer-cron.yaml`; will sync nightly. Or use `ifremer-cron.sh` as a script to run via refular cron if running on a bare, non-kube server.

## Integrity checking

`roundtrip.[py|yaml]` and `Dockerfile-roundtrip` define a pod that will randomly pick profiles from mongo, redownload the ifremer source that defines them, and double checks the collection contents are correct. This is meant to run as a background process to flag errors and demonstrate robustness.

## Historical Context

The rsync in the usage section should work for a from-scratch rebuild as well as nightly updates, but the initial migration from ifremer -> argovis was handled by this repo at commit https://github.com/argovis/ifremer-sync/tree/8df1b7111c07b4e458384ffe8aedf36f27e98b72

## Rebuild mongo argo collections without repeating rsync

If for some reason the rsync'ed mirror is intact but the mongo collections need to be rebuilt from scratch (irrecoverably corrupted or a schema change), see `freshrebuild.sh`. This assumes the rsync mirror can be found in the filesystem at `/ifremer` (ie mount `ifremer-mirror` PVC at `/ifremer`), and will leave you with a file `/tmp/profiles2translate` appropriate for feeding to `testload.sh` to rebuild the `argo` and `argoMeta` collections. This workflow was confirmed to produce _exactly_ the same amount of lines in `/tmp/profiles2translate` as there were argo profiles concurrently in mongo, as it should; future runs should verify this where possible.

Consider parallelizing this by slicing `/tmp/profiles2translate` into equal parts and running one per pod, for example as in `devpod.yaml`.

Note also `testload.sh` has some simple fault tolerance built in, and will try to keep track of progress and resume after an interrupt; checking profiles immediately before and after these breakpoints is the first place to look if an unexpected(ly small) number of profiles appear in the final collection rebuild.

## Manually redo a failed nightly update

If something interrupts a nightly update that finished rsync'ing and parsing the rsync log but was interrupted during the mongo load, best to suspend the cronjob and redo that evening's update; see `redo-sync.yaml` for a pod to manage `ifremer-sync-redo.sh`, which takes the existing `updatedprofiles` list in the logging directory you must specify in the yaml file's command, and reruns the corresponding uploads to mongo.