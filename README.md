# ifremer-sync

This repo contains the scripts and documentation needed to re-sync the entire Argo dataset from ifremer, and re-establish regular imports of this data to Argovis.

## Usage

 - Required volumes:
   - `ifremer-mirror`: contains the files synced from ifremer.
   - `logs`: scratch space for storing logs.
 - Set up as a cronjob using `ifremer-cron.yaml`; will sync nightly.

## Historical Context

The rsync in the usage section should work for a from-scratch rebuild as well as nightly updates, but the initial migration from ifremer -> argovis was handled by this repo at commit https://github.com/argovis/ifremer-sync/tree/8df1b7111c07b4e458384ffe8aedf36f27e98b72