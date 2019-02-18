# encoding: utf-8
require "logstash/config/mixin"

# Tentative of abstracting JDBC logic to a mixin
# for potential reuse in other plugins (input/output)
module LogStash module PluginMixins module JdbcStreaming

  # This method is called when someone includes this module
  def self.included(base)
    # Add these methods to the 'base' given.
    base.extend(self)
    base.setup_jdbc_config
  end

  public
  def setup_jdbc_config
    # JDBC driver library path to third party driver library.
    config :jdbc_driver_library, :validate => :path

    # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
    config :jdbc_driver_class, :validate => :string, :required => true

    # JDBC connection string
    config :jdbc_connection_string, :validate => :string, :required => true

    # JDBC user
    config :jdbc_user, :validate => :string

    # JDBC password
    config :jdbc_password, :validate => :password

    # Connection pool configuration.
    # Validate connection before use.
    config :jdbc_validate_connection, :validate => :boolean, :default => false

    # Connection pool configuration.
    # How often to validate a connection (in seconds)
    config :jdbc_validation_timeout, :validate => :number, :default => 3600

    # Maximum number of times to retry connecting to database.
    # Setting it to 0 (default) disables the whole connection retry mechanism.
    config :connection_retry_attempts, :validate => :number, :default => 0

    # Number of seconds to sleep between connection retry attempts.
    config :connection_retry_attempts_wait_time, :validate => :number, :default => 0.5

    # Minimum number of seconds to wait before the next connection retry cycle starts.
    #
    # While `connection_retry_attempts_wait_time` waits per event and thus blocks the pipeline,
    # `connection_retry_delay` skips connection retries for all events that are processed
    # in the given time frame. The filter fails for these events and the tags from
    # `tag_on_failure` are added.
    config :connection_retry_delay, :validate => :number, :default => 300

    # The delay calculation for the next retry cycle is shared between all
    # *jdbc_streaming* filter instances with the same `global_retry_delay_label`.
    # By default (or set to an empty string) each filter has its own delay.
    config :global_retry_delay_label, :validate => :string
  end

  public
  def prepare_jdbc_connection
    require "sequel"
    require "sequel/adapters/jdbc"
    require "java"

    if @jdbc_driver_library
      class_loader = java.lang.ClassLoader.getSystemClassLoader().to_java(java.net.URLClassLoader)
      class_loader.add_url(java.io.File.new(@jdbc_driver_library).toURI().toURL())
    end

    Sequel::JDBC.load_driver(@jdbc_driver_class)
    jdbc_connect(@connection_retry_attempts + 1)
  end # def prepare_jdbc_connection

  def jdbc_connect(number_of_attempts=@connection_retry_attempts)
    last_exception = nil
    if number_of_attempts.times do |i|
      begin
        @database = Sequel.connect(@jdbc_connection_string, :user=> @jdbc_user, :password=>  @jdbc_password.nil? ? nil : @jdbc_password.value)
        if @jdbc_validate_connection
          @database.extension(:connection_validator)
          @database.pool.connection_validation_timeout = @jdbc_validation_timeout
        end
        @database.test_connection
        break
      rescue ::Sequel::Error => last_exception
        sleep @connection_retry_attempts_wait_time if i < number_of_attempts - 1
      end
    end == number_of_attempts
      raise last_exception
    end
  end # def retry_jdbc_connect
end end end
