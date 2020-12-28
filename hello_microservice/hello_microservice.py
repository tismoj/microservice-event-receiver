from nameko.rpc import rpc

class HelloMicroService:
    name = "hello_microservice"

    @rpc
    def hello(self, data):
        response = {}
        print("In hello: Received JSON data: " + str(data))
        if 'name' in data:
            name = data['name']
            print('Received a request from: ' + name)
            response = {"response": "Hello, {}!".format(name)}
        return {"hello": response}
