psql -c 'create database jdbc_streaming_db;' -U postgres
bundle install
bundle exec rake vendor
bundle exec rspec spec && bundle exec rspec spec --tag integration
