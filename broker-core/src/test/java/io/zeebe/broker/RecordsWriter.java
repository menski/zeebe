package io.zeebe.broker;

import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.test.broker.protocol.clientapi.SubscribedRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

public class RecordsWriter
{
    final List<SubscribedRecord> records;
    private final String path;

    public RecordsWriter(String path, List<SubscribedRecord> records)
    {
        this.path = path;
        this.records = records;
    }

    public void write()
    {
        final String filePath = "/home/zell/tmp/vis/" + path + ".dot";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath)))
        {
            writer.append("digraph incidents\n");
            writer.append("{");
//            writer.append("\nrankdir=LR;");
            writer.append("\nnode [shape=record];\n");
            for (SubscribedRecord subscribedRecord : records)
            {
                final long position = subscribedRecord.position();
                final String valueTypeName = subscribedRecord.valueType().name();
                final String intentName = subscribedRecord.intent().name();
                final long sourceRecordPosition = subscribedRecord.sourceRecordPosition();
                writer.append("\n")
                    .append("" + position)
                    .append(" [label=\"{")
                    .append(valueTypeName)
                    .append("-")
                    .append(intentName)
                    .append("| {position | ").append(""+ subscribedRecord.position()).append("}")
                    .append("| {key | ").append(""+ subscribedRecord.key()).append("}")
                    .append("}\"");

                if (subscribedRecord.recordType() == RecordType.COMMAND)
                {
                    writer.append(", color=red");
                }

                writer.append("];")
                    .append("\n" + position)
                    .append(" -> " + sourceRecordPosition)
                    .append(";");
            }
            writer.append("}");
        }
        catch (Exception ex)
        {
            // yop
            ex.printStackTrace();
        }


    }
}
