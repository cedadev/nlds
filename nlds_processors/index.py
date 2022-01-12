import json
from logging import ERROR

from nlds.rabbit.consumer import RabbitMQConsumer
from nlds.rabbit.publisher import RabbitMQPublisher

class IndexerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = f"{RabbitMQPublisher.RK_ROOT}.{RabbitMQPublisher.RK_INDEX}.{RabbitMQPublisher.RK_WILD}"
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"

    DEFAULT_FILELIST_THRESHOLD = 1000

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        # JEL - probably a nicer way of doing this so user knows to specify 
        # filelist_threshold in the config file. 
        if "filelist_threshold" in self.consumer_config:
            self.threshold = self.consumer_config["filelist_threshold"]
        else: 
            self.threshold = self.DEFAULT_FILELIST_THRESHOLD
    
    def callback(self, ch, method, properties, body, connection):
        try:
            # Convert body from bytes to string for ease of manipulation
            body_json = json.loads(body)

            print(f" [x] Received {body} from {self.queues[0].name}"
                  f"({method.routing_key})")

            # Verify routing key is appropriate
            try:
                rk_parts = self.split_routing_key(method.routing_key)
            except ValueError:
                print(" [XXX] Routing key inappropriate length, exiting callback.")
                return
            
            # Verify filelist is, in fact, a list
            filelist = list(body_json[self.MSG_DATA][self.MSG_FILELIST])
            try:
                filelist_len = len(filelist)
            except TypeError as e:
                print(" [XXX] Filelist cannot be split into sublist, "
                      "incorrect format given.")
                raise e
            
            if rk_parts[2] == self.RK_INITIATE:
                # Split the filelist into batches of 1000 and resubmit
                new_routing_key = ".".join([self.RK_ROOT, self.RK_INDEX, self.RK_INDEX])
                
                if filelist_len > self.threshold:
                    for filesublist in filelist[::self.threshold]:
                        body_json[self.MSG_FILELIST][self.MSG_FILELIST] = filesublist
                        self.publish_message(new_routing_key, json.dumps(body_json))
                else:
                    # Resubmit list as is for indexing
                    self.publish_message(new_routing_key, json.dumps(body_json))

            if rk_parts[2] == self.RK_INDEX:
                if filelist_len > 1000:
                    # TODO: Perhaps allow some dispensation/configuration to 
                    # allow the filelist to be broken down if this does happen?
                    raise ValueError(f"List with larger than allowed length "
                                     f"submitted for indexing ({self.threshold})")
                print(f" [...] Scannning! ")
                # TODO: Dummy filelist inserted, replace this with indexer call
                indexed_filelist = ['dummy', 'file', 'list']

                body_json[self.MSG_DATA][self.MSG_FILELIST] = indexed_filelist
                
                
                print(f" [x] Returning file list to worker and appending route info "
                      f"({self.DEFAULT_REROUTING_INFO})")
                body_json = self.append_route_info(body_json)

                new_routing_key = ".".join([self.RK_ROOT, self.RK_INDEX, self.RK_COMPLETE])
                self.publish_message(new_routing_key, json.dumps(body_json))

            # TODO: Log this?
            print(f" [x] DONE! \n")

        except Exception as e:
            print(f"Encountered error ({e}), sending to monitor.")
            body_json[self.MSG_DATA][self.MSG_ERROR] = str(e)
            new_routing_key = ".".join([self.RK_ROOT, self.RK_MONITOR, self.RK_ERROR])
            self.publish_message(new_routing_key, json.dumps(body_json))
       
        
def main():
    consumer = IndexerConsumer()
    consumer.run()

if __name__ == "__main__":
    main()