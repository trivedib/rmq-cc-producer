# frozen_string_literal: true

require_relative "producer/version"
require "bunny"
require "json"

module Cc
  # class that sends the messages to consumers
  module Producer
    class Error < StandardError; end
    # Your code goes here...
    connection = Bunny.new
    connection.start

    channel = connection.create_channel
    exchange = channel.topic("CC_DATA")
    data = {
      type: "payments",
      old: {
        id: 1234,
        date: "2021-10-25",
        status: "pending"
      },
      new: {
        id: 1234,
        date: "2021-10-28",
        status: "completed"
      }
    }
    # routing key enables us to define multiple queues/consumers
    # and have messaging queue to perform content based routing to different subscribers
    # since we will be having only single consumer i.e. AB, we will keep the routing key as "CC_DATA" same
    # as exchange name.
    # In theory, an exchange can route messages to multiple queues based on routing key when queue registered with
    # same routing key.
    exchange.publish(data.to_json, routing_key: "CC_DATA", persistent: true)
    puts "[producer] Sent message: #{data.to_json}"
  end
end
