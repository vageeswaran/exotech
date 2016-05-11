import socket,sys,json,signal,io
from threading import Timer
from operator import itemgetter
host="127.0.0.1"
port=15000
dataStore={}
def signal_handler(signal, frame):
	print('You pressed Ctrl+C!')
	try:
		with io.open(filename,"w",encoding="utf8") as json_file:
			data=json.dumps(dataStore,ensure_ascii=False)
			json_file.write(unicode(data))
		conn.send("+OK\r\n")
		conn.close()
		sys.exit(0)
	except Exception:
		conn.send("-Error in saving\r\n")


def deleteFunction(x):
	del dataStore[x]
try:
	filename="./exotelredis.db"
	try:
		f=open(filename,"r")
		cont=f.read()
		dataStore=eval(cont)
	except Exception:
		print "no file"
	ssock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	ssock.bind((host,port))
	ssock.listen(5)
	conn,addr=ssock.accept()
	while True:
		try:
			data=conn.recv(1024)
			data=data.split()
			signal.signal(signal.SIGINT, signal_handler)
			if data[0]=="GET":
				if len(data)!=2:
					conn.send("-Error arguments not matched\r\n")
				else:
					try:
						if data[1] in dataStore:
							if "type" in dataStore[data[1]]:
								if dataStore[data[1]]["type"]=="bin":
									conn.send("$"+str(len(dataStore[data[1]]["val"]))+"\r\n"+dataStore[data[1]]["val"]+"\r\n")
							else:
								conn.send("$"+str(len(dataStore[data[1]]))+"\r\n"+dataStore[data[1]]+"\r\n")
						else:
							conn.send("nil\r\n")
					except Exception:
						conn.send("-Error not a string\r\n")

			if data[0]=="SET":
				if len(data)<3:
					conn.send("-Error arguments not matched\r\n")
				elif len(data)==3:
					try:
						dataStore.update({data[1]:data[2]})
						conn.send("+OK\r\n")
					except Exception as e:
						conn.send("-Error not a string\r\n")
				else:
					i=3
					length=len(data)
					while i<length:
						if data[i]=="EX":
							if length>(i+2):
								if data[i+2]=="NX":
									if data[1] in dataStore:
										conn.send("$-1\r\n")
									else:
										dataStore[data[1]]=data[2]
										try:
											#timer here
											Timer(float(data[i+1]),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryNX\r\n")
										
										conn.send("+OK\r\n")
								elif data[i+2]=="XX":
									if data[1] in dataStore:
										dataStore[data[1]]=data[2]
										#timer here
										try:
											#timer here
											Timer(float(data[i+1]),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryXX\r\n")
										
										conn.send("+OK\r\n")
									else:
										conn.send("$-1\r\n")
								i+=3
							else:
								dataStore[data[1]]=data[2]
								#timer here
								try:
									Timer(float(data[i+1]),deleteFunction,(data[1],)).start()
								except Exception:
									conn.send("-Error in ExpiryEX\r\n")
										
								conn.send("+OK\r\n")
								i+=2

						elif data[i]=="PX":
							if length>(i+2):
								if data[i+2]=="NX":
									if data[1] in dataStore:
										conn.send("$-1\r\n")
									else:
										dataStore[data[1]]=data[2]
										#timer here
										try:
											#timer here
											Timer(float(data[i+1])/float(1000),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryPNX\r\n")
										
										conn.send("+OK\r\n")
								elif data[i+2]=="XX":
									if data[1] in dataStore:
										dataStore[data[1]]=data[2]
										#timer here
										try:
											#timer here
											Timer(float(data[i+1])/float(1000),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryPXX\r\n")
										
										conn.send("+OK\r\n")
									else:
										conn.send("$-1\r\n")
								i+=3
							else:
								dataStore[data[1]]=data[2]
								#timer here
								try:
									Timer(float(data[i+1])/float(1000),deleteFunction,(data[1],)).start()
								except Exception:
									conn.send("-Error in ExpiryPX\r\n")
										
								conn.send("+OK\r\n")
								i+=2
						elif data[i]=="NX":
							if data[1] in dataStore:
								i+=3
								conn.send("$-1\r\n")
								
							else:
								if length >(i+2):
									if data[i+1]=="EX":
										dataStore[data[1]]=data[2]
										#timer here
										try:
											Timer(float(data[i+2]),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryEX\r\n")
										
										conn.send("+OK\r\n")
										i+=3
									elif data[i+1]=="PX":
										dataStore[data[1]]=data[2]
										#timer here
										try:
											Timer(float(data[i+2])/float(1000),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryEX\r\n")
										
										conn.send("+OK\r\n")
										i+=3
										
								else:
									dataStore[data[1]]=data[2]									
									conn.send("+OK\r\n")
									i+=1
						elif data[i]=="XX":
							if data[1] in dataStore:
								if length >(i+2):
									if data[i+1]=="EX":
										dataStore[data[1]]=data[2]
										#timer here
										try:
											Timer(float(data[i+2]),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryEX\r\n")
										
										conn.send("+OK\r\n")
										i+=3
									elif data[i+1]=="PX":
										dataStore[data[1]]=data[2]
										#timer here
										try:
											Timer(float(data[i+2])/float(1000),deleteFunction,(data[1],)).start()
										except Exception:
											conn.send("-Error in ExpiryEX\r\n")
										
										conn.send("+OK\r\n")
										i+=3
										
								else:
									dataStore[data[1]]=data[2]									
									conn.send("+OK\r\n")
									i+=1
							else:
								i+=3
								conn.send("$-1\r\n")
								
							
			if data[0]=="GETBIT":
				if len(data)!=3:
					conn.send("-Error in args\r\n")
				else:
					if data[1] in dataStore:
						s=dataStore[data[1]]["val"]
						data[2]=int(data[2])
						idx=data[2]/8
						
						if (len(s)*8)<data[2]:
							conn.send(":0\r\n")
						else:
							val=s[idx]
							val='{:08b}'.format(ord(val))
							conn.send(":"+val[data[2]%8]+"\r\n")
					else:
						conn.send(":0\r\n")

			if data[0]=="SETBIT":
				if len(data)!=4:
					conn.send("-Error in args\r\n")
				else:
					data[2]=int(data[2])
					try:
						if data[1] in dataStore:
							idx=data[2]/8
							if dataStore[data[1]]["type"]=="bin":
								s=dataStore[data[1]]["val"]
								if len(s)*8>data[2]:
									tmp=s[idx]
									tmp=list('{:08b}'.format(ord(tmp)))
									oldVal=tmp[data[2]%8]
									tmp[data[2]%8]=data[3]
									tmp="".join(tmp)
									s=s[:idx]+chr(int(tmp,2))+s[idx+1:]
									dataStore[data[1]]["val"]=s
									conn.send(":"+oldVal+"\r\n")
								else:
									x=7-(data[2]-len(s)*8)%8
									ttmp="0"*(data[2]-(len(s)*8)+x+1)
									ttmp=ttmp[:(data[2]-len(s)*8)]+data[3]+ttmp[(data[2]-len(s)*8)+1:]
									x=""
									i=0
									while i<=len(ttmp)-8:
										x+=chr(int(ttmp[i:i+8],2))
										i+=8
									s+=x
									dataStore[data[1]]["val"]=s
									conn.send(":0\r\n")
							else:
								conn.send("-Error cannot access this variable (Already Exists)\r\n")
						else:
							x=7-data[2]%8
							s="0"*(data[2]+x+1)
							s=s[:data[2]]+data[3]+s[data[2]+1:]
							c={}
							i=0
							x=""
							while i<=len(s)-8:
								x+=chr(int(s[i:i+8],2))
								i+=8
							c["type"]="bin"
							c["val"]=x
							dataStore[data[1]]=c
							conn.send(":0\r\n")
					
					except Exception:
						conn.send("-Error cannot access this variable (Already Exists)\r\n")
			if data[0]=="ZADD":
				if len(data)!=4:
					conn.send("-Error argument not proper\r\n")
				else:
					data[2]=str(float(data[2]))
					if data[1] not in dataStore:						
						dataStore[data[1]]=[[data[3].replace('"',""),data[2]]]
						conn.send(":1\r\n")
					elif data[1] in dataStore and data[2] in dataStore[data[1]] and data[3] in dataStore[data[1]][data[2]]:
						conn.send(":0\r\n")
					elif data[2] not in dataStore[data[1]]:
						dataStore[data[1]]=dataStore[data[1]]+[[data[3].replace('"',""),data[2]]]
						conn.send(":1\r\n")
					else:
						tmp=dataStore[data[1]]
						tmp+=[[data[3].replace('"',""),data[2]]]
						dataStore[data[1]]=dataStore[data[1]]+tmp
						conn.send(":1\r\n")
				dataStore[data[1]]=sorted(dataStore[data[1]],key=itemgetter(1,0))
						
			if data[0]=="ZCARD":
				if len(data)!=2:
					conn.send("-Error argument not proper\r\n")
				else:
					conn.send(":"+str(len(dataStore[data[1]]))+"\r\n")
			

			if data[0]=="ZCOUNT":
				if len(data)!=4:
					conn.send("-Error argument not proper\r\n")
				else:
					if data[1] in dataStore:
						keys=dataStore[data[1]]
						#keys=keys[data[2],data[3]]
						keys=filter(lambda x:float(x[1])>=float(data[2]) and float(x[1])<=float(data[3]),keys)
						keys=map(str,keys)
						count=0
						conn.send(":"+str(len(keys))+"\r\n")
					else:
						conn.send("-Error variable not found\r\n")

			if data[0]=="ZRANGE":
				if len(data)==4:
					if data[1] in dataStore:
						if int(data[3])==-1:
							respstr="*"+str(len(dataStore[data[1]])-int(data[2]))+"\r\n"
							for i in xrange(int(data[2]),len(dataStore[data[1]])):
								vl=dataStore[data[1]][i]
								x=str(type(vl)).split("<type ")[1].split(">")[0].replace("'","").replace('"','')
								respstr+="$"+str(len(vl[0]))+"\r\n"+vl[0]+"\r\n"
							conn.send(respstr)
						else:
							respstr="*"+str((int(data[3])+1)-int(data[2]))+"\r\n"
							for i in xrange(int(data[2]),int(data[3])+1):
								vl=dataStore[data[1]][i]
								x=str(type(vl)).split("<type ")[1].split(">")[0].replace("'","").replace('"','')
								respstr+="$"+str(len(vl[0]))+"\r\n"+vl[0]+"\r\n"
							conn.send(respstr)
					else:
						conn.send("-Error variable not found\r\n")

				elif len(data)==5:
					if data[4]=="WITHSCORES":
						if data[1] in dataStore:
							if int(data[3])==-1:
								respstr="*"+str(len(dataStore[data[1]])-int(data[2]))+"\r\n"
								for i in xrange(int(data[2]),len(dataStore[data[1]])):
									vl=dataStore[data[1]][i]
									x=str(type(vl)).split("<type ")[1].split(">")[0].replace("'","").replace('"','')
									respstr+="$"+str(len(vl[0]))+"\r\n"+vl[0]+"\r\n"+str(vl[1])+"\r\n"
								conn.send(respstr)
							else:
								respstr="*"+str((int(data[3])+1)-int(data[2]))+"\r\n"
								for i in xrange(int(data[2]),int(data[3])+1):
									vl=dataStore[data[1]][i]
									x=str(type(vl)).split("<type ")[1].split(">")[0].replace("'","").replace('"','')
									respstr+="$"+str(len(vl[0]))+"\r\n"+vl[0]+"\r\n"+str(vl[1])+"\r\n"
								conn.send(respstr)
						else:
							conn.send("-Error variable not found\r\n")
					else:
						conn.send("-Error variable not found\r\n")
				else:
					conn.send("-Error variable not found\r\n")
			if data[0]=="SAVE":
				try:
					with io.open(filename,"w",encoding="utf8") as json_file:
						data=json.dumps(dataStore,ensure_ascii=False)
						json_file.write(unicode(data))
					conn.send("+OK\r\n")
				except Exception:
					conn.send("-Error in saving\r\n")
			
		except Exception as e:
			print e
			conn.send("-Error\r\n")
		
except Exception as e:
	sys.exit(0)		
