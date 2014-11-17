package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.ColumnType;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Calendar;

import static org.mysqlmv.cd.logevent.eventdef.data.RowsEventData.*;

/**
 * Created by Kelvin Li on 11/17/2014 11:16 AM.
 */
public abstract class AbstractRowsEventDataParser<T extends RowsEventData> implements EventDataParser<T> {
    private static final int DIG_PER_DEC = 9;
    private static final int[] DIG_TO_BYTES = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

    protected void parseCommon(ByteArrayInputStream input, RowsEventData data) throws IOException {
        data.setTableId(input.readLong(6));
        input.skip(2);
        data.setColumnNum(input.readPackedInteger());
        data.setColumnUsageBeforeUpdate(input.readBitSet(data.getColumnNum(), true));
        // columnUsageAfterUpdate is used for update rows event.
    }

    private RowsEventData.Row parseRow(ByteArrayInputStream input, RowsEventData data) throws IOException {
        RowsEventData.Row row = new RowsEventData.Row();
//        row.setNullColumns(input.readBitSet(getNonZeroLength(data.getColumnUsageBeforeUpdate()), true));
        TableMapEventData tableData = TableMapContext.getTableMap(data.getTableId());
        // get null column bit set.
        BitSet notNullColumn = input.readBitSet(data.getColumnNum(), true);
        for (int i = 0, j = 0; i < data.getColumnNum(); i++) {
            if (!data.getColumnUsageBeforeUpdate().get(i)) {
                continue;
            }
            if (notNullColumn.get(j)) {
                // column j is not null
                int typeCode = tableData.getColumnTypeArray()[j] & 0xFF, meta = tableData.getMetadata()[i], length = 0;
                if (typeCode == ColumnType.STRING.getCode() && meta > 256) {
                    int meta0 = meta >> 8, meta1 = meta & 0xFF;
                    if ((meta0 & 0x30) != 0x30) { // long CHAR field
                        typeCode = meta0 | 0x30;
                        length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
                    } else {
                        if (meta0 == ColumnType.SET.getCode() || meta0 == ColumnType.ENUM.getCode() ||
                                meta0 == ColumnType.STRING.getCode()) {
                            typeCode = meta0;
                            length = meta1;
                        } else {
                            throw new IOException("Unexpected meta " + meta + " for column of type " + typeCode);
                        }
                    }
                }
                row.getCells().add(parseCell(input, getColumnType(tableData.getColumnTypeArray()[j]), tableData.getMetadata()[j], length));
            }
            j++;
        }
        return row;
    }

    private int getNonZeroLength(BitSet bs) {
        int ret = 0;
        for (int i = 0; i < bs.length(); i++) {
            if (bs.get(i)) {
                ret++;
            }
        }
        return ret;
    }

    protected void parseRows(ByteArrayInputStream input, RowsEventData data) throws IOException {
        while (input.available() > 0) {
            data.getRows().add(parseRow(input, data));
        }
    }

    private Cell parseCell(ByteArrayInputStream inputStream, ColumnType columnType, int meta, int length) throws IOException {
        Cell cell = new Cell();
        switch (columnType) {
            case BIT:
                int bitSetLength = (meta >> 8) * 8 + (meta & 0xFF);
                cell.setValue(inputStream.readBitSet(bitSetLength, false));
                break;
            case TINY:
                cell.setValue(inputStream.readInteger(1));
                break;
            case SHORT:
                cell.setValue(inputStream.readInteger(2));
                break;
            case INT24:
                cell.setValue((inputStream.readInteger(3) << 8) >> 8);
                break;
            case LONG:
                cell.setValue(inputStream.readInteger(4));
                break;
            case LONGLONG:
                cell.setValue(inputStream.readLong(8));
                break;
            case FLOAT:
                cell.setValue(Float.intBitsToFloat(inputStream.readInteger(4)));
                break;
            case DOUBLE:
                cell.setValue(Double.longBitsToDouble(inputStream.readLong(8)));
                break;
            case NEWDECIMAL:
                int precision = meta & 0xFF, scale = meta >> 8,
                        decimalLength = determineDecimalLength(precision, scale);
                cell.setValue(toDecimal(precision, scale, inputStream.read(decimalLength)));
                break;
            case DATE:
                cell.setValue(deserializeDate(inputStream));
                break;
            case TIME:
                cell.setValue(deserializeTime(inputStream));
                break;
            case TIME_V2:
                cell.setValue(deserializeTimeV2(meta, inputStream));
                break;
            case TIMESTAMP:
                cell.setValue(deserializeTimestamp(inputStream));
                break;
            case TIMESTAMP_V2:
                cell.setValue(deserializeTimestampV2(meta, inputStream));
                break;
            case DATETIME:
                cell.setValue(deserializeDatetime(inputStream));
                break;
            case DATETIME_V2:
                cell.setValue(deserializeDatetimeV2(meta, inputStream));
                break;
            case YEAR:
                cell.setValue(1900 + inputStream.readInteger(1));
                break;
            case STRING:
                int stringLength = length < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
                cell.setValue(inputStream.readString(stringLength));
                break;
            case VARCHAR:
                break;
            case VAR_STRING:
                int varcharLength = meta < 256 ? inputStream.readInteger(1) : inputStream.readInteger(2);
                cell.setValue(inputStream.readString(varcharLength));
                break;
            case BLOB:
                int blobLength = inputStream.readInteger(meta);
                cell.setValue(inputStream.read(blobLength));
                break;
            case ENUM:
                cell.setValue(inputStream.readInteger(length));
                break;
            case SET:
                cell.setValue(inputStream.readLong(length));
            default:
                throw new IOException("Unsupported type " + columnType);
        }
        cell.setColumnType(columnType);
        return cell;
    }

    private ColumnType getColumnType(byte code) {
        return ColumnType.byCode(code & 0xff);
    }

    private static int determineDecimalLength(int precision, int scale) {
        int x = precision - scale;
        int ipDigits = x / DIG_PER_DEC;
        int fpDigits = scale / DIG_PER_DEC;
        int ipDigitsX = x - ipDigits * DIG_PER_DEC;
        int fpDigitsX = scale - fpDigits * DIG_PER_DEC;
        return (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX] + (fpDigits << 2) + DIG_TO_BYTES[fpDigitsX];
    }

    private static BigDecimal toDecimal(int precision, int scale, byte[] value) {
        boolean positive = (value[0] & 0x80) == 0x80;
        value[0] ^= 0x80;
        if (!positive) {
            for (int i = 0; i < value.length; i++) {
                value[i] ^= 0xFF;
            }
        }
        int x = precision - scale;
        int ipDigits = x / DIG_PER_DEC;
        int ipDigitsX = x - ipDigits * DIG_PER_DEC;
        int ipSize = (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX];
        int offset = DIG_TO_BYTES[ipDigitsX];
        BigDecimal ip = offset > 0 ? BigDecimal.valueOf(bigEndianInteger(value, 0, offset)) : BigDecimal.ZERO;
        for (; offset < ipSize; offset += 4) {
            int i = bigEndianInteger(value, offset, 4);
            ip = ip.movePointRight(DIG_PER_DEC).add(BigDecimal.valueOf(i));
        }
        int shift = 0;
        BigDecimal fp = BigDecimal.ZERO;
        for (; shift + DIG_PER_DEC <= scale; shift += DIG_PER_DEC, offset += 4) {
            int i = bigEndianInteger(value, offset, 4);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIG_PER_DEC));
        }
        if (shift < scale) {
            int i = bigEndianInteger(value, offset, DIG_TO_BYTES[scale - shift]);
            fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
        }
        BigDecimal result = ip.add(fp);
        return positive ? result : result.negate();
    }

    private static int bigEndianInteger(byte[] bytes, int offset, int length) {
        int result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    private static long bigEndianLong(byte[] bytes, int offset, int length) {
        long result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

    private java.sql.Date deserializeDate(ByteArrayInputStream inputStream) throws IOException {
        int value = inputStream.readInteger(3);
        int day = value % 32;
        value >>>= 5;
        int month = value % 16;
        int year = value >> 4;
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DATE, day);
        return new java.sql.Date(cal.getTimeInMillis());
    }

    private static java.sql.Time deserializeTime(ByteArrayInputStream inputStream) throws IOException {
        int value = inputStream.readInteger(3);
        int[] split = split(value, 100, 3);
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(Calendar.HOUR_OF_DAY, split[2]);
        c.set(Calendar.MINUTE, split[1]);
        c.set(Calendar.SECOND, split[0]);
        return new java.sql.Time(c.getTimeInMillis());
    }

    private static int[] split(long value, int divider, int length) {
        int[] result = new int[length];
        for (int i = 0; i < length - 1; i++) {
            result[i] = (int) (value % divider);
            value /= divider;
        }
        result[length - 1] = (int) value;
        return result;
    }

    private java.sql.Time deserializeTimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        /*
            in big endian:

            1 bit sign (1= non-negative, 0= negative)
            1 bit unused (reserved for future extensions)
            10 bits hour (0-838)
            6 bits minute (0-59)
            6 bits second (0-59)
            = (3 bytes in total)
            +
            fractional-seconds storage (size depends on meta)
        */
        long time = bigEndianLong(inputStream.read(3), 0, 3);
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(Calendar.HOUR_OF_DAY, extractBits(time, 2, 10, 24));
        c.set(Calendar.MINUTE, extractBits(time, 12, 6, 24));
        c.set(Calendar.SECOND, extractBits(time, 18, 6, 24));
        c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
        return new java.sql.Time(c.getTimeInMillis());
    }

    private java.util.Date deserializeDatetimeV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        /*
            in big endian:

            1 bit sign (1= non-negative, 0= negative)
            17 bits year*13+month (year 0-9999, month 0-12)
            5 bits day (0-31)
            5 bits hour (0-23)
            6 bits minute (0-59)
            6 bits second (0-59)
            = (5 bytes in total)
            +
            fractional-seconds storage (size depends on meta)
        */
        long datetime = bigEndianLong(inputStream.read(5), 0, 5);
        int yearMonth = extractBits(datetime, 1, 17, 40);
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, yearMonth / 13);
        c.set(Calendar.MONTH, yearMonth % 13 - 1);
        c.set(Calendar.DAY_OF_MONTH, extractBits(datetime, 18, 5, 40));
        c.set(Calendar.HOUR_OF_DAY, extractBits(datetime, 23, 5, 40));
        c.set(Calendar.MINUTE, extractBits(datetime, 28, 6, 40));
        c.set(Calendar.SECOND, extractBits(datetime, 34, 6, 40));
        c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
        return c.getTime();
    }

    private int getFractionalSeconds(int meta, ByteArrayInputStream inputStream) throws IOException {
        int fractionalSecondsStorageSize = getFractionalSecondsStorageSize(meta);
        if (fractionalSecondsStorageSize > 0) {
            long fractionalSeconds = bigEndianLong(inputStream.read(meta), 0, meta);
            if (meta % 2 == 1) {
                fractionalSeconds /= 10;
            }
            return (int) (fractionalSeconds / 1000);
        }
        return 0;
    }

    private static int getFractionalSecondsStorageSize(int fsp) {
        switch (fsp) {
            case 1:
            case 2:
                return 1;
            case 3:
            case 4:
                return 2;
            case 5:
            case 6:
                return 3;
            default:
                return 0;
        }
    }

    private static int extractBits(long value, int bitOffset, int numberOfBits, int payloadSize) {
        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }

    private java.util.Date deserializeDatetime(ByteArrayInputStream inputStream) throws IOException {
        long value = inputStream.readLong(8);
        int[] split = split(value, 100, 6);
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, split[5]);
        c.set(Calendar.MONTH, split[4] - 1);
        c.set(Calendar.DAY_OF_MONTH, split[3]);
        c.set(Calendar.HOUR_OF_DAY, split[2]);
        c.set(Calendar.MINUTE, split[1]);
        c.set(Calendar.SECOND, split[0]);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    private java.sql.Timestamp deserializeTimestamp(ByteArrayInputStream inputStream) throws IOException {
        long value = inputStream.readLong(4);
        return new java.sql.Timestamp(value * 1000L);
    }

    private java.sql.Timestamp deserializeTimestampV2(int meta, ByteArrayInputStream inputStream) throws IOException {
        // big endian
        long timestamp = bigEndianLong(inputStream.read(4), 0, 4);
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp * 1000);
        c.set(Calendar.MILLISECOND, getFractionalSeconds(meta, inputStream));
        return new java.sql.Timestamp(c.getTimeInMillis());
    }
}
