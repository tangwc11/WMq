package com.wentry.wmq.utils.json;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import java.util.ArrayList;
import java.util.List;

public class DynamicFieldSerializerModifier extends BeanSerializerModifier {

    private List<String> fieldsToIgnore;

    public DynamicFieldSerializerModifier(List<String> fieldsToIgnore) {
        this.fieldsToIgnore = fieldsToIgnore;
    }

    @Override
    public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
                                                      BeanDescription beanDesc,
                                                      List<BeanPropertyWriter> properties) {
        List<BeanPropertyWriter> filteredProperties = new ArrayList<>(properties);
        
        for (BeanPropertyWriter prop : properties) {
            if (fieldsToIgnore.contains(prop.getName())) {
                filteredProperties.remove(prop);
            }
        }
        
        return filteredProperties;
    }
}
