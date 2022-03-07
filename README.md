# ifremer-sync

This repo contains the scripts and documentation needed to re-sync the entire Argo dataset from ifremer, and re-establish regular imports of this data to Argovis.

## Recreating the profiles collection from scratch

0. See https://github.com/argovis/db-schema for instructions and resources on how to generate an empty collection with appropriate schema validation.

1. Sync ifremer locally. See the content in the `sync/` directory of this repo, and follow these steps from inside that directory; note you'll need to be logged into your kube cluster from the command line, and that cluster should have available a PVC named `ifremer-mirror` of *at least* 400 GB to accommodate the sync

   ```
   docker image build -t argovis/ifremer-sync:sync .
   docker image push argovis/ifremer-sync:dev
   kubectl apply -f sync.yaml
   ```

2. Choose which files you're going to ingest into mongodb from the initial rsync. From the `parse/` directory, in addition to the `ifremer-mirror` PVC from the previous step, you'll also need a small `logs` PVC:

   ```
   docker image build -t argovis/ifremer-sync:parse .
   docker image push argovis/ifremer-mirror:dev
   kubectl apply -f choosefiles.yaml
   ```

This should create and run a pod that will generate a file `/tmp/profileSelection.txt` in your `logs` PVC; this file contains one line per profile identified, with each line containing the full path to all the files needed to reconstruct that profile, space separated.

3. Populate mongodb with profiles. This step uses the same image as the file selection step, so simply start the pod:

   ```
   kubectl apply -f loadDB.yaml
   ```

   Note WIP: this needs to be parallelized, TBD.

4. In parallel with the previous step, set up and start data integrity tests that doublecheck if the profiles landing in mongo match their upstream nc sources. From `parse/`:

   ```
   docker image build -f Dockerfile-roundtrip -t argovis/ifremer-sync:roundtrip .
   docker image push argovis/ifremer-sync:roundtrip
   kubectl apply -f roundtrip.yaml 
   ```

 This pod is meant to run in the background indefinitely; check its logs periodically for any reports of mismatches found.

5. [WIP] Once initial data loading is complete, rsync again using the `--itemize-changes` flag. Capture the list of new files, and repeat somethign similar to steps 2 and 3 to catch up on all the updates generated since the large initial sync began. You may have to repeat this several times until it takes less than a day.

6. [WIP] set the rsync and update process to proceed at least daily, once it takes less than a day to complete.