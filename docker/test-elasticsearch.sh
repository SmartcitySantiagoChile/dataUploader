
setUp(){
    # Delete all indices
    DELETE_RESPONSE=$(curl -s -X DELETE http://es:9200/*?expand_wildcards=all) 
    assertNotNull $DELETE_RESPONSE
     # Check if all indices was removed
    assertEquals {} $(curl -s -X GET http://es:9200/_cluster/state?filter_path=metadata.indices.*.stat*)

}

testPaymentfactor(){
    python3 datauploader/loadData.py --host es --index paymentfactor tests/files/2019-08-10.paymentfactor &>/dev/null
    PAYMENTFACTOR_DATA=$(curl -X GET http://es:9200/paymentfactor/_search)
    assertNotNull $PAYMENTFACTOR_DATA
}

testBip(){
    python3 datauploader/loadData.py --host es --index bip tests/files/2019-10-07.bip &>/dev/null
    BIP_DATA=$(curl -X GET http://es:9200/bip/_search)
    assertNotNull $BIP_DATA
}

testGeneral(){
    python3 datauploader/loadData.py --host es --index general tests/files/2018-10-01.general &>/dev/null
    GENERAL_DATA=$(curl -X GET http://es:9200/general/_search)
    assertNotNull $GENERAL_DATA
}

testOdbyRoute(){
    python3 datauploader/loadData.py --host es --index odbyroute tests/files/2017-05-08.odbyroute &>/dev/null
    ODBYROUTE_DATA=$(curl -X GET http://es:9200/odbyroute/_search)
    assertNotNull $ODBYROUTE_DATA
}

testOpData(){
    python3 datauploader/loadData.py --host es --index opdata tests/files/2019-03-06.opdata &>/dev/null
    OP_DATA=$(curl -X GET http://es:9200/opdata/_search)
    assertNotNull $OP_DATA
}

testShape(){ 
    python3 datauploader/loadData.py --host es --index shape tests/files/2017-04-03.shape &>/dev/null
    SHAPE_DATA=$(curl -X GET http://es:9200/shape/_search)
    assertNotNull $SHAPE_DATA
}

testSpeed(){
    python3 datauploader/loadData.py --host es --index speed tests/files/2017-04-03.speed &>/dev/null
    SPEED_DATA=$(curl -X GET http://es:9200/speed/_search)
    assertNotNull $SPEED_DATA
}

testStage(){
    python3 datauploader/loadData.py --host es --index stage tests/files/2021-07-26.etapas &>/dev/null
    STAGE_DATA=$(curl -X GET http://es:9200/stage/_search)
    assertNotNull $STAGE_DATA
}

testStop(){
    python3 datauploader/loadData.py --host es --index stop tests/files/2021-05-19.stop &>/dev/null
    STOP_DATA=$(curl -X GET http://es:9200/stop/_search)
    assertNotNull $STOP_DATA
}

testExpedition(){
    python3 datauploader/loadData.py --host es --index expedition tests/files/2016-05-23.expedition &>/dev/null
    EXPEDITION_DATA=$(curl -X GET http://es:9200/expedition/_search)
    assertNotNull $EXPEDITION_DATA
}

testProfile(){
    python3 datauploader/loadData.py --host es --index profile tests/files/2020-03-20.profile &>/dev/null
    PROFILE_DATA=$(curl -X GET http://es:9200/profile/_search)
    assertNotNull $PROFILE_DATA
}

testProfile2(){
    python3 datauploader/loadData.py --host es --index profile tests/files/2021-06-30.profile &>/dev/null
    PROFILE_DATA=$(curl -X GET http://es:9200/profile/_search)
    assertNotNull $PROFILE_DATA
}

testTrip1(){
    python3 datauploader/loadData.py --host es --index trip tests/files/2016-03-14.trip &>/dev/null
    TRIP_DATA=$(curl -X GET http://es:9200/trip/_search)
    assertNotNull $TRIP_DATA
}

testTrip2(){
    python3 datauploader/loadData.py --host es --index trip tests/files/2021-06-30.trip &>/dev/null
    TRIP_DATA=$(curl -X GET http://es:9200/trip/_search)
    assertNotNull $TRIP_DATA
}

estTrip3(){
    python3 datauploader/loadData.py --host es --index trip tests/files/2022-10-01.trip &>/dev/null
    TRIP_DATA=$(curl -X GET http://es:9200/trip/_search)
    assertNotNull $TRIP_DATA
}

