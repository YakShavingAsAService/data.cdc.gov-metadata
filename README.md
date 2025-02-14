# data.cdc.gov-metadata

A script to add supplementary metadata to data.cdc.gov dataset downloads from January 2025.

The downloads are available at https://cdcdotgovarchive.org/CDC_datasets/.

Methodology:

In January 2025, the sitemaps.xml files for data.cdc.gov were used to come up with a list of dataset homepages served by that host. From there, the datasets themselves were downloaded (download_filename, downloaded_ts).

The socrata id was inferred from the dataset homepage's URL, and the downloaded filename became: socrata-id_download-timestamp-as-unix-epoch_name-downloaded-file-name.

To construct the rest of this documentation, socrata metadata was queried for the id. If found, the socrata name metadata became the dataset_name column, and the socrata_description became the description column. All the socrata metadata is a single string in the additional metadata column. The socrata metadata was queried on Feb. 13. 2025. If the socrata metadata query was unsuccessful, the "UNKN_SOC_NAME" name was used.

The dataset homepages include metadata about the datasets themselves and change over time. The Internet Archive has snapshots of these homepages, and links to the snapshots are in the Internet Archive snapshots column. The data are ordered by dataset name and may contain more than one dataset endpoint, identified by socrata id.
