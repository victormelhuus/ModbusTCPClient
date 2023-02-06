from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder, Endian
from pymodbus.exceptions import ModbusException, ModbusIOException, ParameterException,NoSuchSlaveException
from queue import Queue
from threading import Event


class ModbusClient():
    def __init__(self,config):
        self.health = 0
        self.config = config
        self.meters = config["meters"]
        self.error = dict()
        self.status = 'init'

    def read(self, tags, q:Queue, e:Event):
        """
        tags: dict() of values to append
        q:Queue status queue for UI
        e:Event close/abort event
        """
        #Loop through all meters
        i = 0
        num_meters = len(self.meters)
        for meter in self.meters:
            if e.is_set():
                break

            #Connect
            meterIP = meter["ip"]
            client = ModbusTcpClient(meterIP)
            q.put("m|"+ progressString(percent=int(i/num_meters), width=40, name = "Reading"))
            i += 1
            client.connect()    

            #Get information about meter
            items = list(meter['values'].keys())
            uuid = meter['uuid']
            measure_name = meter['type']
            slave_id = meter['id']
            meter_tags = dict()
            
            for item in items: #For all register read requests in "values"
                if e.is_set():
                    break
                #Get info
                regtype = meter['values'][item]['registerType']
                reg = meter['values'][item]['register']
                size = meter['values'][item]['size']
                scale = meter['values'][item]['scale'] 
                type = meter['values'][item]['type']

                match meter['values'][item]['byteorder']:
                    case "big":
                        byteorder = Endian.Big
                    case "little":
                        byteorder = Endian.Little

                match meter['values'][item]['wordorder']:
                    case "big":
                        wordorder = Endian.Big
                    case "little":
                        wordorder = Endian.Little  

                #Read                      
                try:
                    match regtype:
                        case "holding":
                            res = client.read_holding_registers(reg,size, slave=slave_id)
                        case "input":
                            res = client.read_input_registers(reg,size, slave=slave_id)
                        case _:
                            raise Exception("Wrong registerType naming, must be holding or input")

                    #Check error
                    errorMsg = meter["uuid"]+" "+meter["ip"]+" "+item+" "+str(reg)
                    err = res.isError()
                    if err:
                        self.error[errorMsg] = err
                        reason = []
                        if isinstance(res, ModbusIOException):
                            reason = "error resulting from data i/o"
                        elif isinstance(res, ModbusException):
                            reason = "base error"
                        elif isinstance(res, ParameterException):
                            reason = "error resulting from invalid parameter"
                        elif isinstance(res, NoSuchSlaveException):
                            reason = "error resulting from making a request to a slave that does not exist"
                        self.status = "Modbus error: " + errorMsg + " Reason: " + reason
                    else: #SUCCESS! Decode message
                        if errorMsg in list(self.error.keys()):
                            self.error.pop(errorMsg)
                        decoder = BinaryPayloadDecoder.fromRegisters(res.registers, byteorder=byteorder, wordorder=wordorder)
                        match type:
                            case "16float":
                                val =  decoder.decode_16bit_float()
                            case "16int":
                                val =  decoder.decode_16bit_int()
                            case "16uint":
                                val =  decoder.decode_16bit_uint()
                            case "32float":
                                val =  decoder.decode_32bit_float()
                            case "32int":
                                val =  decoder.decode_32bit_int()
                            case "32uint":
                                val =  decoder.decode_32bit_uint()
                            case "64float":
                                val =  decoder.decode_64bit_float()
                            case "64int":
                                val =  decoder.decode_64bit_int()
                            case "64uint":
                                val =  decoder.decode_64bit_uint()
                            case _:
                                val =  Exception("Error decoding, illegal type string" + errorMsg)
                        meter_tags[item] = val*scale
                except Exception as E: #Capture any exceptions
                    self.error[meterIP] = str(E)
                    break
            client.close()
            if e.is_set():
                break
            meter_tags['measure_name'] = measure_name
            tags[uuid] = meter_tags
        self.health = len(list(self.error.keys())) #Update health as number of errors
        return tags

def progressString(percent=0, width=40, name = "progress",end=""):
    left = width * percent // 100
    right = width - left
    
    tags = "#" * left
    spaces = " " * right
    percents = f"{percent:.0f}%"
    return name+": " + "[" + tags + spaces + "]" + percents +end
                

                
             