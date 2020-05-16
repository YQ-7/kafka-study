package com.study.kafka.consumer;

import com.study.kafka.producer.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * 使用Protostuff反序列化
 */
public class CompanyProtostuffDeserializer implements Deserializer<Company> {
    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data, company, schema);
        return company;
    }
}
