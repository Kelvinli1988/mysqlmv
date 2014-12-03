package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.ColumnType;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 9:31 AM.
 */
public class TableMapEventDataParser implements EventDataParser<TableMapEventData> {
    @Override
    public TableMapEventData parse(ByteArrayInputStream stream) throws IOException {
        TableMapEventData data = new TableMapEventData();
        data.setTableID(stream.readLong(6));
        stream.skip(2);
        data.setDbNameLength(stream.readInteger(1));
        data.setDbName(stream.readZeroTerminatedString());
        data.setTableNameLength(stream.readInteger(1));
        data.setTableName(stream.readZeroTerminatedString());
        data.setColumnNum(stream.readPackedInteger());
        data.setColumnTypeArray(stream.read(data.getColumnNum()));
        data.setMetaDataLength(stream.readPackedInteger());
        data.setMetadata(readMetadata(stream, data.getColumnTypeArray()));
        data.setColumnNullable(stream.readBitSet(data.getColumnNum(), true));
        return data;
    }


    private int[] readMetadata(ByteArrayInputStream inputStream, byte[] columnTypes) throws IOException {
        int[] metadata = new int[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            switch(ColumnType.byCode(columnTypes[i] & 0xFF)) {
                case FLOAT:
                case DOUBLE:
                case BLOB:
                    metadata[i] = inputStream.readInteger(1);
                    break;
                case BIT:
                case VARCHAR:
                case NEWDECIMAL:
                    metadata[i] = inputStream.readInteger(2);
                    break;
                case SET:
                case ENUM:
                case STRING:
                    metadata[i] = bigEndianInteger(inputStream.read(2), 0, 2);
                    break;
                case TIME_V2:
                case DATETIME_V2:
                case TIMESTAMP_V2:
                    metadata[i] = inputStream.readInteger(1); // fsp (@see {@link ColumnType})
                    break;
                default:
                    metadata[i] = 0;
            }
        }
        return metadata;
    }

    private static int bigEndianInteger(byte[] bytes, int offset, int length) {
        int result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }
}
