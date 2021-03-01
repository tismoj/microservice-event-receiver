from nameko.rpc import rpc


class Hello2MicroService:
    name = "hello2_microservice"

    @rpc
    def hello2(self, name):
        print('Received a request from: ' + name)
        return "Hello, {}!".format(name)
