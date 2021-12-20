import json
from logging import ERROR

from nlds.utils.constants import DATA, DATA_FILELIST
from nlds.nlds_setup import INDEX_FILELIST_THRESHOLD
from nlds_processors.utils.constants import INITIATE, MONITOR
from utils.constants import COMPLETE, ROOT, INDEX, WILD
from nlds.rabbit.consumer import RabbitMQConsumer

class IndexerConsumer(RabbitMQConsumer):
    DEFAULT_QUEUE_NAME = "index_q"
    DEFAULT_ROUTING_KEY = f"{ROOT}.{INDEX}.{WILD}"
    DEFAULT_REROUTING_INFO = f"->INDEX_Q"

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)
    
    def callback(self, ch, method, properties, body, connection):
        try:
            # Convert body from bytes to string for ease of manipulation
            body_json = json.loads(body)

            print(f" [x] Received {body} from {self.queues[0].name}"
                  f"({method.routing_key})")

            # Verify routing key is appropriate
            try:
                rk_parts = self.verify_routing_key(method.routing_key)
            except ValueError:
                print(" [XXX] Routing key inappropriate length, exiting callback.")
                return
            
            # Verify filelist is, in fact, a list
            filelist = list(body_json[DATA][DATA_FILELIST])
            try:
                filelist_len = len(filelist)
            except TypeError as e:
                print(" [XXX] Filelist cannot be split into sublist, "
                      "incorrect format given.")
                raise e
            
            if rk_parts[2] == INITIATE:
                # Split the filelist into batches of 1000 and resubmit
                new_routing_key = ".".join([ROOT, INDEX, INDEX])
                
                if filelist_len > INDEX_FILELIST_THRESHOLD:
                    for filesublist in filelist[::INDEX_FILELIST_THRESHOLD]:
                        body_json[DATA][DATA_FILELIST] = filesublist
                        self.publish_message(new_routing_key, json.dumps(body_json))
                else:
                    # Resubmit list as is for indexing
                    self.publish_message(new_routing_key, json.dumps(body_json))

            if rk_parts[2] == INDEX:
                if filelist_len > 1000:
                    # TODO: Perhaps allow some dispensation/configuration to 
                    # allow the filelist to be broken down if this does happen?
                    raise ValueError(f"List with larger than allowed length "
                                     f"submitted for indexing ({INDEX_FILELIST_THRESHOLD})")
                print(f" [...] Scannning! ")
                # TODO: Dummy filelist inserted, replace this with indexer call
                indexed_filelist = ['dummy', 'file', 'list']

                body_json[DATA][DATA_FILELIST] = indexed_filelist
                
                
                print(f" [x] Returning file list to worker and appending route info "
                      f"({self.DEFAULT_REROUTING_INFO})")
                body_json = self.append_route_info(body_json)

                new_routing_key = ".".join([ROOT, INDEX, COMPLETE])
                self.publish_message(new_routing_key, json.dumps(body_json))

            # TODO: Log this?
            print(f" [x] DONE! \n")

        except Exception as e:
            print(f"Encountered error ({e}), sending to monitor.")
            body_json[DATA][ERROR] = str(e)
            new_routing_key = ".".join([ROOT, MONITOR, ERROR])
            self.publish_message(new_routing_key, json.dumps(body_json))
       
        
if __name__ == "__main__":
    consumer = IndexerConsumer()
    consumer.run()