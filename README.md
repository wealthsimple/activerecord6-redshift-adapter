activerecord6-redshift-adapter
==============================

Amazon Redshift adapter for ActiveRecord 6 (Rails 6).
I cloned the project from ConsultingMD/activerecord5-redshift-adapter.

The Redshift driver for previous ActiveRecord versions already exist:
ActiveRecord5: https://github.com/ConsultingMD/activerecord5-redshift-adapter
ActiveRecord4: https://github.com/aamine/activerecord4-redshift-adapter
ActiveRecord3: https://github.com/fiksu/activerecord-redshift-adapter

There are some differences for AR6, and this driver tries to stay compatible
with those.

If anybody writes a better Redshift driver which works with ActiveRecord 6,
and maintains it, we would happily switch to that.

Usage
-------------------

For Rails 6, write following in Gemfile:
```
gem 'activerecord6-redshift-adapter'
```

In database.yml
```
development:
  adapter: redshift
  host: your_cluster_name.at.redshift.amazonaws.com
  port: 5439
  database: your_db
  username: your_user
  password: your_password
  encoding: utf8
  pool: 3
  timeout: 5000
```

License
---------

MIT license (same as ActiveRecord)
