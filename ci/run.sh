bundle install
bundle exec rake vendor
USER=logstash bundle exec rspec spec
USER=postgres bundle exec rspec spec --tag integration
