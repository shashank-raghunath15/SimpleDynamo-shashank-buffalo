package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import java.util.Date;

/**
 * Created by shash on 2/22/2018.
 */

public class Database extends SQLiteOpenHelper {

    public static final String DB_NAME = "messages.db";
    public static final String TABLE_NAME = "msg";
    public static final String KEY = "key";
    public static final String VAL = "value";
    public static final String VERSION = "version";

    public Database(Context context) {
        super(context, DB_NAME, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("create table " + TABLE_NAME + " ( "
                + KEY + " TEXT, "
                + VAL + " TEXT, "
                + VERSION + " TEXT "
                + " , CONSTRAINT unique_key UNIQUE (" + KEY + "))");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(sqLiteDatabase);
    }

    public boolean insert(ContentValues contentValues) {
        Cursor cursor = query(contentValues.getAsString(KEY));

        if (cursor.moveToFirst()) {
            String v = cursor.getString(2);
            int v1 = Integer.valueOf(contentValues.getAsString(VERSION));
            int v2 = Integer.valueOf(v);

            if (v1 > v2) {
                delete(contentValues.getAsString(KEY));
                SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
                contentValues.put(VERSION, String.valueOf(v2++));
                return sqLiteDatabase.insert(TABLE_NAME, null, contentValues) == -1 ? false : true;
            } else {
                return false;
            }
        } else {
            SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
            return sqLiteDatabase.insert(TABLE_NAME, null, contentValues) == -1 ? false : true;
        }
    }

    public Cursor getAllMessages() {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        return sqLiteDatabase.rawQuery("select key, value from " + TABLE_NAME, null);
    }

    public Cursor query(String selectionArg) {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        Cursor cursor = sqLiteDatabase.rawQuery("select * from " + TABLE_NAME + " WHERE " + KEY + " = '" + selectionArg + "'", null);
        return cursor;
    }

    public void delete(String key) {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        sqLiteDatabase.delete(TABLE_NAME, KEY + " = '" + key + "'", null);
    }

    public void deleteAll() {
        SQLiteDatabase sqLiteDatabase = this.getWritableDatabase();
        sqLiteDatabase.execSQL("DELETE FROM " + TABLE_NAME);
    }
}
