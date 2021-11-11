from ..rabbit_connection import RabbitMQConnection

rabbit_connection = RabbitMQConnection()

def rabbit_publish_response(transaction_id, action, contents):
    rabbit_connection.get_connection()
    msg = rabbit_connection.create_message(transaction_id, action, contents)
    rabbit_connection.publish_message(msg)

