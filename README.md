# openalex-mariadb-toolkit

NOTE: before you can load the .csv files into your database you must download, flatten, and decompress them!
- how to download: https://docs.openalex.org/download-snapshot/download-to-your-machine
- Python script to flatten: https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
- decompress as needed using a tool of your choice (e.g. Linux gunzip, Windows: 7zip)

These SQL commands will:
- create the OpenALex (openalex.org) schema in MariaDB
- load 'flattened' csv files into the database

Schema creation is forked from the Postgres gist at: https://gist.github.com/richard-orr/4c30f52cf5481ac68dc0b282f46f1905 with modifications to work with MariaDB conventions (timestamps do not store time zone, keys require defined lengths for each subcomponent) and syntax

File loading is forked from the Postgres gist at: https://gist.github.com/richard-orr/a1117d7dd618970a1af23fa4b54c4da4 with modifications to work with MariaDB syntax
