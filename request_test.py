import requests

class csms_api_slave:
    def __init__(self, redis_resource_id_lst, uri, headers={"Content-Type": "application/json"},):
        self.redis_resource_id_lst = redis_resource_id_lst.copy()
        self.uri = uri
        self.headers = headers
        # resource_id que
        # Remove the element by value
        for i in redis_resource_id_lst:
            # print(i)
            try:
                if resource_id_set.check_element(i):
                    pass
                else:
                    self.redis_resource_id_lst.remove(i)    
            except ValueError:
                self.redis_resource_id_lst.remove(i)
                print(f"Element {redis_resource_id} has been taken.")
        print(f"charger_slave is online: {self.redis_resource_id_lst}")

    def restricting_charger(self, redis_instance):
        restricting_charger_cal = get_from_redis_json(f"restricting_charger_cal", redis_instance)
        # print(restricting_charger_cal)
        # print(self.redis_resource_id_lst)
        
        if restricting_charger_cal:
            for _id in self.redis_resource_id_lst:
                
                try:
                    for redis_id in self.redis_resource_id_lst:
                        _id = redis_id.split(":")[1]
                        _charger_id = _id.split("_")[0]
                        _connector_id = _id.split("_")[1]
                        _endpoint = f"/ocpp/SmartCharging/SetChargingProfile/chargers/{_charger_id}/connectors/{_connector_id}"
                        if restricting_charger_cal.get(_id):
                            _set_power = restricting_charger_cal.get(_id)
                            _url = self.uri + _endpoint
                            _data = {
                                    "chargingProfileId": 0,
                                    "transactionId": 0,
                                    "stackLevel": 0,
                                    "schedulerPeriodList": [
                                        {
                                        "startPeriod": 0,
                                        "limit": _set_power
                                        }
                                    ]
                                    }
                            _resp = requests.post(_url, json=json.dumps(_data), headers=self.headers)
                            print(f"restricting_charger:{_id} Suc!")
                        else:
                            pass
                    return self.csms_write_0_populate(restricting_charger_cal)

                except Exception as e:
                    print(f"restricting_charger Error occurred: {e}")
                    return None
        else:
            print("chargespot or restricting_charger_cal is None")
            return None