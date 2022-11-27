# openalex-mysql-mariadb-toolkit

NOTE: this approach 'dehydrates' the substring "https://openalex.org/" out of id values to save space/data transfer/computation, assuming you will 'rehydrate' it at the point of end use when you want to refer to the object on openalex.org

Prerequisites:
- MySQL or MariaDB running on the localhost
- Python
- Download the OpenAlex snapshot: https://docs.openalex.org/download-snapshot/download-to-your-machine

Use the Python script to flatten the data into tab-delimited (.tsv) files (since commas can appear in text fields, tabs proved more reliable).  Each content type runs in its own thread.  Concepts, venues, and institutions should return quickly.  Authors will likely take a few hours and works may take several hours, depending on your machine.

The SQL commands will:
- create the OpenAlex (openalex.org) schema, with some adjustments for MySQL / MariaDB
- load 'flattened' tsv files into the database

Credit:
Schema creation is forked from the Postgres gist at: https://gist.github.com/richard-orr/4c30f52cf5481ac68dc0b282f46f1905 with modifications to work with MySQL / MariaDB conventions (timestamps do not store time zone, keys require defined lengths for each subcomponent) and syntax

File loading is forked from the Postgres gist at: https://gist.github.com/richard-orr/a1117d7dd618970a1af23fa4b54c4da4 with modifications to work with MySQL MariaDB syntax and use of tab delimiters
