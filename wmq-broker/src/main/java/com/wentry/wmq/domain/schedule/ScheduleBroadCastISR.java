package com.wentry.wmq.domain.schedule;

import com.wentry.wmq.domain.BrokerState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Description:
 * @Author: tangwc
 */
@Component
public class ScheduleBroadCastISR {

    @Autowired
    BrokerState brokerState;


    public void init(){

    }

}
