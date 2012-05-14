
#coding=gbk
#!/usr/bin/env python
import pymongo
import os
import sys
sys.path.append('/home/data/thrift/gen-py')
from sys import getsizeof, stderr
from itertools import chain
from collections import deque

from maimiaotech import Segment
from maimiaotech.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from pymongo import Connection

type_flag=False
noSenseWords=[]
type_specialToalnum=False


def total_size(o, handlers={}, verbose=False):
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     
    seen = set()                      
    default_size = getsizeof(0)      

    def sizeof(o):
        if id(o) in seen:      
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def initlog():
    import logging
   
    logger = logging.getLogger()
    hdlr = logging.FileHandler("./logger_txt")
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.NOTSET)
    return logger


def element_cmp(E1, E2):
    return cmp(E1["recomed"], E2["recomed"])
def query_same_pv_cmp(E1,E2):
    return cmp(E1["pv"],E2["pv"])

def strFull_to_Half(ustring):
    rstring = ""
    for uchar in ustring:
        inside_code=ord(uchar)
        if inside_code==0x3000:
            inside_code=0x0020
        else:
            inside_code-=0xfee0
        if inside_code<0x0020 or inside_code>0x7e:
            rstring += uchar
        else:
            rstring += unichr(inside_code)
    return rstring

def is_chinese(uchar):
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
            return True
    else:
            return False
def is_number(uchar):
    if uchar >= u'\u0030' and uchar<=u'\u0039':
            return True
    else:
            return False
def is_alphabet(uchar):
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
            return True
    else:
            return False
def is_other(uchar):
    if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
         return True
    else:
         return False

def judgeTypespecialChar(Term):
    TypeSpecialChar="-/*&"
    for char in TypeSpecialChar:
        if char in Term:
              return True
    return False

def judgeType(Term):
    if ((Term.isalnum() or judgeTypespecialChar(Term)) and len(Term)>2 and (Term!="2012" and Term!="2011")):
       return True
    else:return False

def edit_dist(first,second):
    if len(first) > len(second):
        first,second = second,first
    if len(first) == 0:
        return len(second)
    if len(second) == 0:
        return len(first)
    first_length = len(first) + 1
    second_length = len(second) + 1
    distance_matrix = [range(second_length) for x in range(first_length)]
    #print distance_matrix 
    for i in range(1,first_length):
        for j in range(1,second_length):
            deletion = distance_matrix[i-1][j] + 1
            insertion = distance_matrix[i][j-1] + 1
            substitution = distance_matrix[i-1][j-1]
            if first[i-1] != second[j-1]:
                substitution += 1
            distance_matrix[i][j] = min(insertion,deletion,substitution)
#    print distance_matrix
    return distance_matrix[first_length-1][second_length-1]                                    


def calTypeMatchLength(offer_terms,query_terms):
    minLength=0
    temp=0
    global type_specialToalnum
    type_specialToalnum=False
    for query in query_terms:
          if judgeType(query["term"]):
              for offer in offer_terms:
                  if (judgeType(offer["term"])):
                        temp_length=edit_dist(offer["term"],query["term"])
                        temp+=1
                        if (temp_length==1 and abs(len(offer["term"])-len(query["term"]))==1):
                              if((judgeTypespecialChar(offer["term"]) and query["term"].isalnum()) or (judgeTypespecialChar(query["term"]) and offer["term"].isalnum())):
                                  type_specialToalnum=True  
                        if temp==1:
                            minLength=temp_length
                        else:
                            if temp_length<minLength:
                                  minLength=temp_length
    if temp>0:
        return minLength
    else: return 100

def calTypeRelevance(offer_terms,query_terms):
    score=0.00
    final_score=0.00
    global type_flag
    type_flag=False
    for query in query_terms:
        if (query["term"].isalnum() and len(query["term"])>2 and (query["term"]!="2012" and query["term"]!="2011")):
             for offer in offer_terms:
                   if (offer["term"].isalnum() and len(offer["term"])>2):
                         temp_length=edit_dist(offer["term"],query["term"])
                         #if(temp_length<=edit_length):
                        # max_length=(offer["term"]>=query["term"] ? offer["term"]:query["term"])
                         if len(offer["term"])>=len(query["term"]):
                               max_length=len(offer["term"])
                         else:
                               max_length=len(query["term"])
                         temp_score=(max_length-temp_length)/(max_length+0.00001)
                         if temp_score >score:
                              score=temp_score
                         if (query["nType"]=="XH" and offer["term"]=="XH"):
                                type_flag=True
        if score>final_score:
           final_score=score
    return final_score
             
def calRelevanceScore(offer_terms,query_terms,noSenseWords):
    hitCount=0
    noSenseHitCount=0
    noSenseCount=0
    for no in noSenseWords:
       for a in query_terms:      
               if a["term"]==no:
                   noSenseCount+=1;               
    for a in query_terms:
        flag_1=0
        for b in offer_terms:
           if a["term"]==b["term"]:
                hitCount+=1
                if b["term"] in noSenseWords:
                   noSenseHitCount+=1
                flag_1=1
                break
        if not flag_1:
             if a["nType"]=="xh":
                for b_1 in offer_terms:
                    if b_1["key"].find(a["key"])!=-1:
                         hitCount+=1
   # print hitCount,noSenseHitCount,len(query_terms),noSenseCount
    score=(hitCount-noSenseHitCount)/(len(query_terms)-noSenseCount+0.000001)
    if score>1:
         score=1.0
    #print "%.5f"%(score)
    return score

def noSenseWords_init():
    global noSenseWords
    noSenseWords.append(" ".encode('utf-8'))
    noSenseWords.append(",".encode('utf-8'))
    noSenseWords.append("'".encode('utf-8'))
    #noSenseWords.append(u"包邮".encode('utf-8'))
    noSenseWords.append(u"正品".encode('utf-8'))
    noSenseWords.append(u"专柜".encode('utf-8'))
    noSenseWords.append(u"新款".encode('utf-8'))
    noSenseWords.append(u"新品".encode('utf-8'))

def calFinalScore(text_score,edit_length):
    if edit_length<2:

         if ((edit_length==0) or (edit_length==1 and type_specialToalnum==True)):
            if text_score>0.98:score=text_score
            elif text_score>0.70:score=text_score*1.5
            elif text_score>=0.5:score=text_score*1.6
            else:score=text_score*2.0
         else :
            if text_score>0.98:score=text_score
            elif text_score>0.70:score=text_score*1.2
            elif text_score>=0.5:score=text_score*1.3
            else:score=text_score*1.6
    elif edit_length<4:score=text_score
    else: score=text_score*0.9
    if score>1.0:score=0.9999
    return score

def getResult_all(shopname):
    connection=Connection('localhost',1996)
    exec("db=connection.%s"%(shopname))
    coll=db.tb_items

    connection2=Connection('localhost',1995)
    db2=connection2.test
    coll2=db2.querycat
    coll3=db2.queryall
    offer=[]
    global noSenseWords 
    for data in coll.find({},{"_id":1,"title":1,"catid":1,"sales":1}):
       offer.append({"title":data["title"],"catid":data["catid"],"sales":data["sales"],"_id":data["_id"]})
    try:
         transport = TSocket.TSocket('localhost', 1991)
         transport = TTransport.TBufferedTransport(transport)
         protocol = TBinaryProtocol.TBinaryProtocol(transport)
         client = Segment.Client(protocol)
         transport.open()
         termlist_query_catId=[]
         offer_catId=[]
         iter=0;iter1=0
         result_all=[]
        # debugInfo=file("debugInfo",'w')
         for each_offer in offer:
                offer_one=strFull_to_Half(each_offer["title"].lower()).encode('utf-8')
                logger.info(each_offer["catid"].encode("utf8")+offer_one)
                termslist_offer = client.seg_process(offer_one)
                logger.info(" len offer_segprocess is %d",len(termslist_offer))
                offer_one_terms=[]
                for term in termslist_offer:
                         offer_one_terms.append({"term":term.key,"nWeight":term.nWeight, "nType":term.nType})
                query_all=[]
                if not each_offer["catid"] in offer_catId:
                       iter+=1
                       offer_catId.append(each_offer["catid"])
                       
                     #  print "offer id is:%s" % each_offer["catid"]
              #for data_query in coll2.find({"cat.c":each_offer["catid"],"cat.w":{"$lt":"100","$gt":"0"}},{"_id":1}):
                       for data_query in coll2.find({"cat":{"$elemMatch":{"c":each_offer["catid"],"w":{"$gt":"20"}}}},{"_id":1}):
                              query_one=strFull_to_Half(data_query["_id"].lower().encode('utf-8'))
                              query_all.append(query_one)
                       query_all_terms=[]
                       logger.info("for each item len of query_all is %d",len(query_all))
                       for query_one in query_all:
                             # logger.info(each_offer["catid"].encode("utf8")+query_one)
                              query_one_terms=[]
                              termslist_query=client.seg_process(query_one)
                              for term in termslist_query:
                                     query_one_terms.append({"term":term.key,"nWeight":term.nWeight, "nType":term.nType})
                              text_rele_score=calRelevanceScore(offer_one_terms,query_one_terms,noSenseWords)
                              edit_length=calTypeMatchLength(offer_one_terms,query_one_terms)
                              score=calFinalScore(text_rele_score,edit_length)
                              query_all_terms.append({"queryone":query_one,"queryoneterms":query_one_terms})
                              if score>0.81:
                                     for query_pv in coll3.find({"_id":query_one},{"pv":1}):
                                              element={}
                                              element["query"] = query_one
                                              element["offer"]=offer_one
                                              element["_id"]=each_offer["_id"]
                                              if int(query_pv["pv"])>400: pv_score=0
                                              elif int(query_pv["pv"])>200: pv_score=1
                                              elif int(query_pv["pv"])>100: pv_score=2
                                              elif int(query_pv["pv"])>9: pv_score=3
                                              else:  pv_score=0
                                              if int(each_offer["sales"])>20: sale_score=2
                                              elif int(each_offer["sales"])>5: sale_score=1
                                              else: sale_score=0
                                              element["recomed"]=(pv_score+sale_score) 
                                              if pv_score==0: element["recomed"]=0
                                              if not element in result_all: 
                                                     result_all.append(element)
                                          #   print pv_score,sale_score,element["recomed"]
                              #logger.info(score)
                       if total_size(termlist_query_catId)<2*1024*1024*1024:
                                termlist_query_catId.append({"offer_catID":each_offer["catid"],"query_catId_terms":query_all_terms})
                else:
                       for termlist_catID in termlist_query_catId:
                               if each_offer["catid"]==termlist_catID["offer_catID"]:
                                     for tgc in termlist_catID["query_catId_terms"]:
                                         text_rele_score=calRelevanceScore(offer_one_terms,tgc["queryoneterms"],noSenseWords)
                                         edit_length=calTypeMatchLength(offer_one_terms,tgc["queryoneterms"])
                                         score=calFinalScore(text_rele_score,edit_length)
                                       #  logger.info(tgc["queryone"])
                                         if score>0.81:
                                             for query_pv in coll3.find({"_id":tgc["queryone"]},{"pv":1}):
                                                  element={}
                                                  element["query"] = tgc["queryone"]
                                                  element["offer"]=offer_one
                                                  element["_id"]=each_offer["_id"]
                                                  if int(query_pv["pv"])>400: pv_score=0;
                                                  elif int(query_pv["pv"])>200: pv_score=1
                                                  elif int(query_pv["pv"])>100: pv_score=2
                                                  elif int(query_pv["pv"])>9: pv_score=3;
                                                  else: pv_score=0
                                                  if int(each_offer["sales"])>20:sale_score=2
                                                  elif int(each_offer["sales"])>5:sale_score=1
                                                  else: sale_score=0
                                                  element["recomed"] =(pv_score+sale_score)
                                                  if pv_score==0: element["recomed"]=0
                                                  if not element in result_all:
                                                      result_all.append(element)
                                        # logger.info(score)
                                     iter1+=1
                                     break
               # sys.exit(0)
    except Thrift.TException, tx:
         print 'Exception:%s' % (tx.message)
   # print iter,iter1
    logger.info("iter:"+str(iter)+" iter1:"+str(iter1))
    return result_all

def get_result_recomd(result_all,Ncount=200):
    result_all.sort(element_cmp,reverse=True)
    query_offer_map={}
    count=0
    gt0_count=0
    final_result=[]
    logger.info("len of result_all is %d",len(result_all))
    for result in result_all:
         if result["recomed"] >0: 
               gt0_count+=1
               if query_offer_map.has_key(result["query"]):
                  query_offer_map[result["query"]]+=1
                  if query_offer_map[result["query"]]>2:
                      continue;
                  if not result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n" in final_result: 
                      final_result.append(result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n")
                      count+=1
               else:
                   query_offer_map[result["query"]]=1
                   if not result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n"  in final_result:
                        final_result.append(result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n")
                        count+=1
              # logger.info(result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n")
         
        # elif result["recomed"]==0:
               #logger.info("recomd 0 is "+result["_id"].encode("utf-8")+","+result["offer"]+","+result["query"]+"\n")
       # if count==Ncount:
            #  break
    logger.info("final count is %d,gt0_count is %d",count,gt0_count)
    csvFile=open("test_hotwind.csv","w")       
    for one_result in final_result:
          csvFile.write(one_result)
    return final_result

def main_program(shopname,Ncount=200):
    result_all=getResult_all(shopname)
    final_result=get_result_recomd(result_all,Ncount)
    return final_result

if __name__=='__main__':
    logger = initlog()
    logger.error(50)
    logger.info("ck_info")
#    main_program("lclp168",40)
    main_program("hotwind")
