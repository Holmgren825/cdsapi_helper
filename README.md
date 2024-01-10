# cdsapi_helper
This is a small utility package for working with the [cdsapi](https://github.com/ecmwf/cdsapi) from ECMWF.

The main feature is the `download_cds` CLI.
This reads a `.toml` specification of what you want to download, sends the requests and stores the request ids in a `.csv` for later use, i.e. updating status of the request and downloading the data.

You can specify one, or more, variables which should be looped over. Say you want to download multiple years, but in separate files.

Work in progress.
