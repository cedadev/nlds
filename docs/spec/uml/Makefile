message_flow_get%.png : message_flow_get%.puml
	plantuml $<

message_flow_put%.png : message_flow_put%.puml
	plantuml $<

all : message_flow_get1.png message_flow_get2.png message_flow_put1.png message_flow_put2.png message_flow_put3.png

clean:
	rm message_flow_get?.png
	rm message_flow_put?.png