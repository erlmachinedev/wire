PROJECT = wire
PROJECT_DESCRIPTION = "Electric power transmission based on RabbitMq"
PROJECT_VERSION = 0.0.1

DEPS = erlmachine amqp_client

TEST_DEPS = meck

dep_erlmachine = git https://github.com/Erlmachine/erlmachine
dep_amqp_client = hex 3.8.14

dep_meck = git https://github.com/eproxus/meck.git 0.9.0

include erlang.mk
