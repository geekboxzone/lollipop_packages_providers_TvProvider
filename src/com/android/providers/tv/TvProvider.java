/*
 * Copyright (C) 2014 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.providers.tv;

import android.content.ComponentName;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.media.tv.TvContract;
import android.media.tv.TvContract.BaseTvColumns;
import android.media.tv.TvContract.Channels;
import android.media.tv.TvContract.Programs;
import android.media.tv.TvContract.WatchedPrograms;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.ParcelFileDescriptor;
import android.os.ParcelFileDescriptor.AutoCloseInputStream;
import android.text.TextUtils;
import android.util.Log;

import libcore.io.IoUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

/**
 * TV content provider. The contract between this provider and applications is defined in
 * {@link android.media.tv.TvContract}.
 */
public class TvProvider extends ContentProvider {
    // STOPSHIP: Turn debugging off.
    private static final boolean DEBUG = true;
    private static final String TAG = "TvProvider";

    private static final UriMatcher sUriMatcher;
    private static final int MATCH_CHANNEL = 1;
    private static final int MATCH_CHANNEL_ID = 2;
    private static final int MATCH_CHANNEL_ID_LOGO = 3;
    private static final int MATCH_CHANNEL_ID_PROGRAM = 4;
    private static final int MATCH_INPUT_PACKAGE_SERVICE_CHANNEL = 5;
    private static final int MATCH_PROGRAM = 6;
    private static final int MATCH_PROGRAM_ID = 7;
    private static final int MATCH_WATCHED_PROGRAM = 8;
    private static final int MATCH_WATCHED_PROGRAM_ID = 9;

    private static final String SELECTION_OVERLAPPED_PROGRAM = Programs.COLUMN_CHANNEL_ID
            + "=? AND " + Programs.COLUMN_START_TIME_UTC_MILLIS + "<=? AND "
            + Programs.COLUMN_END_TIME_UTC_MILLIS + ">=?";

    private static final String SELECTION_CHANNEL_BY_INPUT = Channels.COLUMN_PACKAGE_NAME
            + "=? AND " + Channels.COLUMN_SERVICE_NAME + "=?";

    private static final String LOGO_DIRECTORY = "logo";
    private static final int MAX_LOGO_IMAGE_SIZE = 256;

    private static HashMap<String, String> sChannelProjectionMap;
    private static HashMap<String, String> sProgramProjectionMap;
    private static HashMap<String, String> sWatchedProgramProjectionMap;

    static {
        sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
        sUriMatcher.addURI(TvContract.AUTHORITY, "channel", MATCH_CHANNEL);
        sUriMatcher.addURI(TvContract.AUTHORITY, "channel/#", MATCH_CHANNEL_ID);
        sUriMatcher.addURI(TvContract.AUTHORITY, "channel/#/logo", MATCH_CHANNEL_ID_LOGO);
        sUriMatcher.addURI(TvContract.AUTHORITY, "channel/#/program", MATCH_CHANNEL_ID_PROGRAM);
        sUriMatcher.addURI(TvContract.AUTHORITY, "input/*/*/channel",
                MATCH_INPUT_PACKAGE_SERVICE_CHANNEL);
        sUriMatcher.addURI(TvContract.AUTHORITY, "program", MATCH_PROGRAM);
        sUriMatcher.addURI(TvContract.AUTHORITY, "program/#", MATCH_PROGRAM_ID);
        sUriMatcher.addURI(TvContract.AUTHORITY, "watched_program", MATCH_WATCHED_PROGRAM);
        sUriMatcher.addURI(TvContract.AUTHORITY, "watched_program/#", MATCH_WATCHED_PROGRAM_ID);

        sChannelProjectionMap = new HashMap<String, String>();
        sChannelProjectionMap.put(Channels._ID, Channels._ID);
        sChannelProjectionMap.put(Channels.COLUMN_PACKAGE_NAME, Channels.COLUMN_PACKAGE_NAME);
        sChannelProjectionMap.put(Channels.COLUMN_SERVICE_NAME, Channels.COLUMN_SERVICE_NAME);
        sChannelProjectionMap.put(Channels.COLUMN_TYPE, Channels.COLUMN_TYPE);
        sChannelProjectionMap.put(Channels.COLUMN_TRANSPORT_STREAM_ID,
                Channels.COLUMN_TRANSPORT_STREAM_ID);
        sChannelProjectionMap.put(Channels.COLUMN_DISPLAY_NUMBER, Channels.COLUMN_DISPLAY_NUMBER);
        sChannelProjectionMap.put(Channels.COLUMN_DISPLAY_NAME, Channels.COLUMN_DISPLAY_NAME);
        sChannelProjectionMap.put(Channels.COLUMN_DESCRIPTION, Channels.COLUMN_DESCRIPTION);
        sChannelProjectionMap.put(Channels.COLUMN_BROWSABLE, Channels.COLUMN_BROWSABLE);
        sChannelProjectionMap.put(Channels.COLUMN_INTERNAL_PROVIDER_DATA,
                Channels.COLUMN_INTERNAL_PROVIDER_DATA);
        sChannelProjectionMap.put(Channels.COLUMN_VERSION_NUMBER, Channels.COLUMN_VERSION_NUMBER);

        sProgramProjectionMap = new HashMap<String, String>();
        sProgramProjectionMap.put(Programs._ID, Programs._ID);
        sProgramProjectionMap.put(Programs.COLUMN_PACKAGE_NAME, Programs.COLUMN_PACKAGE_NAME);
        sProgramProjectionMap.put(Programs.COLUMN_CHANNEL_ID, Programs.COLUMN_CHANNEL_ID);
        sProgramProjectionMap.put(Programs.COLUMN_TITLE, Programs.COLUMN_TITLE);
        sProgramProjectionMap.put(Programs.COLUMN_START_TIME_UTC_MILLIS,
                Programs.COLUMN_START_TIME_UTC_MILLIS);
        sProgramProjectionMap.put(Programs.COLUMN_END_TIME_UTC_MILLIS,
                Programs.COLUMN_END_TIME_UTC_MILLIS);
        sProgramProjectionMap.put(Programs.COLUMN_SHORT_DESCRIPTION,
                Programs.COLUMN_SHORT_DESCRIPTION);
        sProgramProjectionMap.put(Programs.COLUMN_LONG_DESCRIPTION,
                Programs.COLUMN_LONG_DESCRIPTION);
        sProgramProjectionMap.put(Programs.COLUMN_INTERNAL_PROVIDER_DATA,
                Programs.COLUMN_INTERNAL_PROVIDER_DATA);
        sProgramProjectionMap.put(Programs.COLUMN_VERSION_NUMBER, Programs.COLUMN_VERSION_NUMBER);

        sWatchedProgramProjectionMap = new HashMap<String, String>();
        sWatchedProgramProjectionMap.put(WatchedPrograms._ID, WatchedPrograms._ID);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_WATCH_START_TIME_UTC_MILLIS,
                WatchedPrograms.COLUMN_WATCH_START_TIME_UTC_MILLIS);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_WATCH_END_TIME_UTC_MILLIS,
                WatchedPrograms.COLUMN_WATCH_END_TIME_UTC_MILLIS);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_CHANNEL_ID,
                WatchedPrograms.COLUMN_CHANNEL_ID);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_TITLE,
                WatchedPrograms.COLUMN_TITLE);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_START_TIME_UTC_MILLIS,
                WatchedPrograms.COLUMN_START_TIME_UTC_MILLIS);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_END_TIME_UTC_MILLIS,
                WatchedPrograms.COLUMN_END_TIME_UTC_MILLIS);
        sWatchedProgramProjectionMap.put(WatchedPrograms.COLUMN_DESCRIPTION,
                WatchedPrograms.COLUMN_DESCRIPTION);
    }

    private static final int DATABASE_VERSION = 3;
    private static final String DATABASE_NAME = "tv.db";
    private static final String CHANNELS_TABLE = "channels";
    private static final String PROGRAMS_TABLE = "programs";
    private static final String WATCHED_PROGRAMS_TABLE = "watched_programs";
    private static final String DEFAULT_CHANNELS_SORT_ORDER = Channels.COLUMN_DISPLAY_NUMBER
            + " ASC";
    private static final String DEFAULT_PROGRAMS_SORT_ORDER = Programs.COLUMN_START_TIME_UTC_MILLIS
            + " ASC";
    private static final String DEFAULT_WATCHED_PROGRAMS_SORT_ORDER =
            WatchedPrograms.COLUMN_WATCH_START_TIME_UTC_MILLIS + " DESC";

    private static final String PERMISSION_ALL_EPG_DATA = "android.permission.ALL_EPG_DATA";

    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            if (DEBUG) {
                Log.d(TAG, "Creating database");
            }
            // Set up the database schema.
            db.execSQL("CREATE TABLE " + CHANNELS_TABLE + " ("
                    + Channels._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
                    + Channels.COLUMN_PACKAGE_NAME + " TEXT NOT NULL,"
                    + Channels.COLUMN_SERVICE_NAME + " TEXT NOT NULL,"
                    + Channels.COLUMN_TYPE + " INTEGER NOT NULL DEFAULT 0,"
                    + Channels.COLUMN_SERVICE_TYPE + " INTEGER NOT NULL DEFAULT 1,"
                    + Channels.COLUMN_ORIGINAL_NETWORK_ID + " INTEGER,"
                    + Channels.COLUMN_TRANSPORT_STREAM_ID + " INTEGER,"
                    + Channels.COLUMN_SERVICE_ID + " INTEGER,"
                    + Channels.COLUMN_DISPLAY_NUMBER + " TEXT,"
                    + Channels.COLUMN_DISPLAY_NAME + " TEXT,"
                    + Channels.COLUMN_DESCRIPTION + " TEXT,"
                    + Channels.COLUMN_BROWSABLE + " INTEGER NOT NULL DEFAULT 1,"
                    + Channels.COLUMN_INTERNAL_PROVIDER_DATA + " BLOB,"
                    + Channels.COLUMN_VERSION_NUMBER + " INTEGER"
                    + ");");
            db.execSQL("CREATE TABLE " + PROGRAMS_TABLE + " ("
                    + Programs._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
                    + Programs.COLUMN_PACKAGE_NAME + " TEXT NOT NULL,"
                    + Programs.COLUMN_CHANNEL_ID + " INTEGER,"
                    + Programs.COLUMN_TITLE + " TEXT,"
                    + Programs.COLUMN_START_TIME_UTC_MILLIS + " INTEGER,"
                    + Programs.COLUMN_END_TIME_UTC_MILLIS + " INTEGER,"
                    + Programs.COLUMN_SHORT_DESCRIPTION + " TEXT,"
                    + Programs.COLUMN_LONG_DESCRIPTION + " TEXT,"
                    + Programs.COLUMN_POSTER_ART_URI + " TEXT,"
                    + Programs.COLUMN_THUMBNAIL_URI + " TEXT,"
                    + Programs.COLUMN_INTERNAL_PROVIDER_DATA + " BLOB,"
                    + Programs.COLUMN_VERSION_NUMBER + " INTEGER,"
                    + "FOREIGN KEY(" + Programs.COLUMN_CHANNEL_ID + ") REFERENCES "
                            + CHANNELS_TABLE + "(" + Channels._ID + ")"
                    + ");");
            db.execSQL("CREATE TABLE " + WATCHED_PROGRAMS_TABLE + " ("
                    + WatchedPrograms._ID + " INTEGER PRIMARY KEY AUTOINCREMENT,"
                    + WatchedPrograms.COLUMN_WATCH_START_TIME_UTC_MILLIS + " INTEGER,"
                    + WatchedPrograms.COLUMN_WATCH_END_TIME_UTC_MILLIS + " INTEGER,"
                    + WatchedPrograms.COLUMN_CHANNEL_ID + " INTEGER,"
                    + WatchedPrograms.COLUMN_TITLE + " TEXT,"
                    + WatchedPrograms.COLUMN_START_TIME_UTC_MILLIS + " INTEGER,"
                    + WatchedPrograms.COLUMN_END_TIME_UTC_MILLIS + " INTEGER,"
                    + WatchedPrograms.COLUMN_DESCRIPTION + " TEXT,"
                    + "FOREIGN KEY(" + WatchedPrograms.COLUMN_CHANNEL_ID + ") REFERENCES "
                            + CHANNELS_TABLE + "(" + Channels._ID + ")"
                    + ");");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            if (DEBUG) {
                Log.d(TAG, "Upgrading database from " + oldVersion + " to " + newVersion);
            }

            // Default upgrade case.
            db.execSQL("DROP TABLE IF EXISTS " + CHANNELS_TABLE);
            db.execSQL("DROP TABLE IF EXISTS " + PROGRAMS_TABLE);
            db.execSQL("DROP TABLE IF EXISTS " + WATCHED_PROGRAMS_TABLE);
            onCreate(db);
        }
    }

    private DatabaseHelper mOpenHelper;
    private File mLogoPath;

    @Override
    public boolean onCreate() {
        if (DEBUG) {
          Log.d(TAG, "Creating TvProvider");
        }
        mOpenHelper = new DatabaseHelper(getContext());
        mLogoPath = new File(getContext().getFilesDir(), LOGO_DIRECTORY);
        return true;
    }

    @Override
    public String getType(Uri uri) {
        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL:
                return Channels.CONTENT_TYPE;
            case MATCH_CHANNEL_ID:
                return Channels.CONTENT_ITEM_TYPE;
            case MATCH_CHANNEL_ID_LOGO:
                return "image/png";
            case MATCH_CHANNEL_ID_PROGRAM:
                return Programs.CONTENT_TYPE;
            case MATCH_INPUT_PACKAGE_SERVICE_CHANNEL:
                return Channels.CONTENT_TYPE;
            case MATCH_PROGRAM:
                return Programs.CONTENT_TYPE;
            case MATCH_PROGRAM_ID:
                return Programs.CONTENT_ITEM_TYPE;
            case MATCH_WATCHED_PROGRAM:
                return WatchedPrograms.CONTENT_TYPE;
            case MATCH_WATCHED_PROGRAM_ID:
                return WatchedPrograms.CONTENT_ITEM_TYPE;
            default:
                throw new IllegalArgumentException("Unknown URI " + uri);
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (needsToLimitPackage(uri)) {
            if (!TextUtils.isEmpty(selection)) {
                throw new IllegalArgumentException("Selection not allowed for " + uri);
            }
            selection = BaseTvColumns.COLUMN_PACKAGE_NAME + "=?";
            selectionArgs = new String[] { getCallingPackage() };
        }

        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        String orderBy;

        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL:
                queryBuilder.setTables(CHANNELS_TABLE);
                queryBuilder.setProjectionMap(sChannelProjectionMap);
                orderBy = DEFAULT_CHANNELS_SORT_ORDER;
                break;
            case MATCH_CHANNEL_ID:
                queryBuilder.setTables(CHANNELS_TABLE);
                queryBuilder.setProjectionMap(sChannelProjectionMap);
                selection = DatabaseUtils.concatenateWhere(selection, Channels._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                orderBy = DEFAULT_CHANNELS_SORT_ORDER;
                break;
            case MATCH_CHANNEL_ID_PROGRAM:
                queryBuilder.setTables(PROGRAMS_TABLE);
                queryBuilder.setProjectionMap(sProgramProjectionMap);
                selection = DatabaseUtils.concatenateWhere(selection,
                        Programs.COLUMN_CHANNEL_ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        TvContract.getChannelId(uri)
                });
                String paramStartTime = uri.getQueryParameter(TvContract.PARAM_START_TIME);
                String paramEndTime = uri.getQueryParameter(TvContract.PARAM_END_TIME);
                if (paramStartTime != null && paramEndTime != null) {
                    String startTime = String.valueOf(Long.parseLong(paramStartTime));
                    String endTime = String.valueOf(Long.parseLong(paramEndTime));
                    selection = DatabaseUtils.concatenateWhere(selection,
                            SELECTION_OVERLAPPED_PROGRAM);
                    selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                            TvContract.getChannelId(uri), endTime, startTime
                    });
                }
                orderBy = DEFAULT_PROGRAMS_SORT_ORDER;
                break;
            case MATCH_INPUT_PACKAGE_SERVICE_CHANNEL:
                queryBuilder.setTables(CHANNELS_TABLE);
                queryBuilder.setProjectionMap(sChannelProjectionMap);
                boolean browsableOnly = uri.getBooleanQueryParameter(
                        TvContract.PARAM_BROWSABLE_ONLY, true);
                selection = DatabaseUtils.concatenateWhere(selection, SELECTION_CHANNEL_BY_INPUT
                        + (browsableOnly ? " AND " + Channels.COLUMN_BROWSABLE + "=1" : ""));
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        TvContract.getPackageName(uri), TvContract.getServiceName(uri)
                });
                orderBy = DEFAULT_CHANNELS_SORT_ORDER;
                break;
            case MATCH_PROGRAM:
                queryBuilder.setTables(PROGRAMS_TABLE);
                queryBuilder.setProjectionMap(sProgramProjectionMap);
                orderBy = DEFAULT_PROGRAMS_SORT_ORDER;
                break;
            case MATCH_PROGRAM_ID:
                queryBuilder.setTables(PROGRAMS_TABLE);
                queryBuilder.setProjectionMap(sProgramProjectionMap);
                selection = DatabaseUtils.concatenateWhere(selection, Programs._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                orderBy = DEFAULT_PROGRAMS_SORT_ORDER;
                break;
            case MATCH_WATCHED_PROGRAM:
                queryBuilder.setTables(WATCHED_PROGRAMS_TABLE);
                queryBuilder.setProjectionMap(sWatchedProgramProjectionMap);
                orderBy = DEFAULT_WATCHED_PROGRAMS_SORT_ORDER;
                break;
            case MATCH_WATCHED_PROGRAM_ID:
                queryBuilder.setTables(WATCHED_PROGRAMS_TABLE);
                queryBuilder.setProjectionMap(sWatchedProgramProjectionMap);
                selection = DatabaseUtils.concatenateWhere(selection, WatchedPrograms._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                orderBy = DEFAULT_WATCHED_PROGRAMS_SORT_ORDER;
                break;
            default:
                throw new IllegalArgumentException("Unknown URI " + uri);
        }

        // Use the default sort order only if no sort order is specified.
        if (!TextUtils.isEmpty(sortOrder)) {
            orderBy = sortOrder;
        }

        // Get the database and run the query.
        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        Cursor c = queryBuilder.query(db, projection, selection, selectionArgs, null, null,
                orderBy);

        // Tell the cursor what URI to watch, so it knows when its source data changes.
        c.setNotificationUri(getContext().getContentResolver(), uri);
        return c;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL:
            case MATCH_CHANNEL_ID:
                return insertChannel(uri, values);
            case MATCH_PROGRAM:
            case MATCH_PROGRAM_ID:
                return insertProgram(uri, values);
            case MATCH_WATCHED_PROGRAM:
            case MATCH_WATCHED_PROGRAM_ID:
                return insertWatchedProgram(uri, values);
            default:
                throw new IllegalArgumentException("Unknown URI " + uri);
        }
    }

    private Uri insertChannel(Uri uri, ContentValues values) {
        validateServiceName(values.getAsString(Channels.COLUMN_SERVICE_NAME));

        // Mark the owner package of this channel.
        values.put(Channels.COLUMN_PACKAGE_NAME, getCallingPackage());

        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long rowId = db.insert(CHANNELS_TABLE, null, values);
        if (rowId > 0) {
            Uri channelUri = TvContract.buildChannelUri(rowId);
            getContext().getContentResolver().notifyChange(channelUri, null);
            return channelUri;
        }

        throw new SQLException("Failed to insert row into " + uri);
    }

    private Uri insertProgram(Uri uri, ContentValues values) {
        // Mark the owner package of this program.
        values.put(Programs.COLUMN_PACKAGE_NAME, getCallingPackage());

        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long rowId = db.insert(PROGRAMS_TABLE, null, values);
        if (rowId > 0) {
            Uri programUri = TvContract.buildProgramUri(rowId);
            getContext().getContentResolver().notifyChange(programUri, null);
            return programUri;
        }

        throw new SQLException("Failed to insert row into " + uri);
    }

    private Uri insertWatchedProgram(Uri uri, ContentValues values) {
        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        long rowId = db.insert(WATCHED_PROGRAMS_TABLE, null, values);
        if (rowId > 0) {
            Uri watchedProgramUri = TvContract.buildWatchedProgramUri(rowId);
            getContext().getContentResolver().notifyChange(watchedProgramUri, null);
            return watchedProgramUri;
        }

        throw new SQLException("Failed to insert row into " + uri);
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (needsToLimitPackage(uri)) {
            if (!TextUtils.isEmpty(selection)) {
                throw new IllegalArgumentException("Selection not allowed for " + uri);
            }
            selection = BaseTvColumns.COLUMN_PACKAGE_NAME + "=?";
            selectionArgs = new String[] { getCallingPackage() };
        }

        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int count = 0;

        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL:
                count = db.delete(CHANNELS_TABLE, selection, selectionArgs);
                break;
            case MATCH_CHANNEL_ID:
                selection = DatabaseUtils.concatenateWhere(selection, Channels._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.delete(CHANNELS_TABLE, selection, selectionArgs);
                break;
            case MATCH_CHANNEL_ID_PROGRAM:
                String paramStartTime = uri.getQueryParameter(TvContract.PARAM_START_TIME);
                String paramEndTime = uri.getQueryParameter(TvContract.PARAM_END_TIME);
                if (paramStartTime != null && paramEndTime != null) {
                    String startTime = String.valueOf(Long.parseLong(paramStartTime));
                    String endTime = String.valueOf(Long.parseLong(paramEndTime));
                    selection = DatabaseUtils.concatenateWhere(selection,
                            SELECTION_OVERLAPPED_PROGRAM);
                    selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                            TvContract.getChannelId(uri), endTime, startTime
                    });
                    count = db.delete(PROGRAMS_TABLE, selection, selectionArgs);
                    if (count > 1) {
                        Log.e(TAG, "Deleted more than one current program");
                    }
                } else {
                    selection = DatabaseUtils.concatenateWhere(selection, Programs.COLUMN_CHANNEL_ID
                            + "=?");
                    selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                            TvContract.getChannelId(uri)
                    });
                    count = db.delete(PROGRAMS_TABLE, selection, selectionArgs);
                }
                break;
            case MATCH_INPUT_PACKAGE_SERVICE_CHANNEL:
                boolean browsableOnly = uri.getBooleanQueryParameter(
                        TvContract.PARAM_BROWSABLE_ONLY, true);
                selection = DatabaseUtils.concatenateWhere(selection, SELECTION_CHANNEL_BY_INPUT
                        + (browsableOnly ? " AND " + Channels.COLUMN_BROWSABLE + "=1" : ""));
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        TvContract.getPackageName(uri), TvContract.getServiceName(uri)
                });
                count = db.delete(CHANNELS_TABLE, selection, selectionArgs);
                break;
            case MATCH_PROGRAM:
                count = db.delete(PROGRAMS_TABLE, selection, selectionArgs);
                break;
            case MATCH_PROGRAM_ID:
                selection = DatabaseUtils.concatenateWhere(selection, Programs._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.delete(PROGRAMS_TABLE, selection, selectionArgs);
                break;
            case MATCH_WATCHED_PROGRAM:
                count = db.delete(WATCHED_PROGRAMS_TABLE, selection, selectionArgs);
                break;
            case MATCH_WATCHED_PROGRAM_ID:
                selection = DatabaseUtils.concatenateWhere(selection, WatchedPrograms._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.delete(WATCHED_PROGRAMS_TABLE, selection, selectionArgs);
                break;
            default:
                throw new IllegalArgumentException("Unknown URI " + uri);
        }

        if (count > 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return count;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        if (needsToLimitPackage(uri)) {
            if (!TextUtils.isEmpty(selection)) {
                throw new IllegalArgumentException("Selection not allowed for " + uri);
            }
            selection = BaseTvColumns.COLUMN_PACKAGE_NAME + "=?";
            selectionArgs = new String[] { getCallingPackage() };
        }

        SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        int count = 0;

        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL:
                count = db.update(CHANNELS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_CHANNEL_ID:
                selection = DatabaseUtils.concatenateWhere(selection, Channels._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.update(CHANNELS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_CHANNEL_ID_PROGRAM:
                String paramStartTime = uri.getQueryParameter(TvContract.PARAM_START_TIME);
                String paramEndTime = uri.getQueryParameter(TvContract.PARAM_END_TIME);
                if (paramStartTime != null && paramEndTime != null) {
                    String startTime = String.valueOf(Long.parseLong(paramStartTime));
                    String endTime = String.valueOf(Long.parseLong(paramEndTime));
                    selection = DatabaseUtils.concatenateWhere(selection,
                            SELECTION_OVERLAPPED_PROGRAM);
                    selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                            TvContract.getChannelId(uri), endTime, startTime
                    });
                    count = db.update(PROGRAMS_TABLE, values, selection, selectionArgs);
                    if (count > 1) {
                        Log.e(TAG, "Updated more than one current program");
                    }
                } else {
                    selection = DatabaseUtils.concatenateWhere(selection, Programs.COLUMN_CHANNEL_ID
                            + "=?");
                    selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                            TvContract.getChannelId(uri)
                    });
                    count = db.update(PROGRAMS_TABLE, values, selection, selectionArgs);
                }
                break;
            case MATCH_INPUT_PACKAGE_SERVICE_CHANNEL:
                boolean browsableOnly = uri.getBooleanQueryParameter(
                        TvContract.PARAM_BROWSABLE_ONLY, true);
                selection = DatabaseUtils.concatenateWhere(selection, SELECTION_CHANNEL_BY_INPUT
                        + (browsableOnly ? " AND " + Channels.COLUMN_BROWSABLE + "=1" : ""));
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        TvContract.getPackageName(uri), TvContract.getServiceName(uri)
                });
                count = db.update(CHANNELS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_PROGRAM:
                count = db.update(PROGRAMS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_PROGRAM_ID:
                selection = DatabaseUtils.concatenateWhere(selection, Programs._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.update(PROGRAMS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_WATCHED_PROGRAM:
                count = db.update(WATCHED_PROGRAMS_TABLE, values, selection, selectionArgs);
                break;
            case MATCH_WATCHED_PROGRAM_ID:
                selection = DatabaseUtils.concatenateWhere(selection, WatchedPrograms._ID + "=?");
                selectionArgs = DatabaseUtils.appendSelectionArgs(selectionArgs, new String[] {
                        uri.getLastPathSegment()
                });
                count = db.update(WATCHED_PROGRAMS_TABLE, values, selection, selectionArgs);
                break;
            default:
                throw new IllegalArgumentException("Unknown URI " + uri);
        }

        if (count > 0) {
            getContext().getContentResolver().notifyChange(uri, null);
        }
        return count;
    }

    private boolean needsToLimitPackage(Uri uri) {
        // If an application is trying to access channel or program data, we need to ensure that the
        // access is limited to only those data entries that the application provided in the first
        // place. The only exception is when the application has the full data access. Note that the
        // user's watch log is treated separately with a special permission.
        int match = sUriMatcher.match(uri);
        return match != MATCH_WATCHED_PROGRAM && match != MATCH_WATCHED_PROGRAM_ID
                && !callerHasFullEpgAccess();
    }

    private boolean callerHasFullEpgAccess() {
        return getContext().checkCallingPermission(PERMISSION_ALL_EPG_DATA)
                == PackageManager.PERMISSION_GRANTED;
    }

    private void validateServiceName(String serviceName) {
        String packageName = getCallingPackage();
        ComponentName componentName = new ComponentName(packageName, serviceName);
        try {
            getContext().getPackageManager().getServiceInfo(componentName, 0);
        } catch (PackageManager.NameNotFoundException e) {
            throw new IllegalArgumentException("Invalid service name: " + serviceName);
        }
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        switch (sUriMatcher.match(uri)) {
            case MATCH_CHANNEL_ID_LOGO:
                return openLogoFile(uri, mode);
            default:
                throw new FileNotFoundException(uri.toString());
        }
    }

    private ParcelFileDescriptor openLogoFile(Uri uri, String mode) throws FileNotFoundException {
        long channelId = Long.parseLong(uri.getPathSegments().get(1));
        if (mode.equals("r")) {
            File file = getLogoFile(channelId);
            if (!file.exists()) {
                throw new FileNotFoundException("No logo found for channel ID " + channelId);
            }
            return ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY);
        } else {
            if (!callerOwnsChannelId(channelId)) {
                throw new FileNotFoundException(uri.toString());
            }
            try {
                ParcelFileDescriptor[] pipeFds = ParcelFileDescriptor.createPipe();
                PipeMonitor pipeMonitor = new PipeMonitor(channelId, pipeFds[0]);
                pipeMonitor.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
                return pipeFds[1];
            } catch (IOException ioe) {
                FileNotFoundException fne = new FileNotFoundException(
                        "Could not create temp logo file for channel ID" + channelId);
                fne.initCause(ioe);
                throw fne;
            }
        }
    }

    private File getLogoFile(long channelId) {
        return new File(mLogoPath, String.valueOf(channelId));
    }

    private boolean callerOwnsChannelId(long channelId) {
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(CHANNELS_TABLE);
        queryBuilder.setProjectionMap(sChannelProjectionMap);

        String[] projection = new String[] { Channels._ID };
        String selection = Channels._ID + "=? AND " + Channels.COLUMN_PACKAGE_NAME + "=?";
        String[] selectionArgs = new String[] { String.valueOf(channelId), getCallingPackage() };

        SQLiteDatabase db = mOpenHelper.getReadableDatabase();
        Cursor cursor = queryBuilder.query(
                db, projection, selection, selectionArgs, null, null, null);
        try {
            return cursor.getCount() == 1;
        } finally {
            cursor.close();
        }
    }

    private class PipeMonitor extends AsyncTask<Void, Void, Void> {
        private final long mChannelId;
        private final ParcelFileDescriptor mPfd;

        private PipeMonitor(long channelId, ParcelFileDescriptor pfd) {
            mChannelId = channelId;
            mPfd = pfd;
        }

        @Override
        protected Void doInBackground(Void... params) {
            AutoCloseInputStream is = new AutoCloseInputStream(mPfd);
            File tempFile = null;
            FileOutputStream fos = null;
            try {
                Bitmap bitmap = BitmapFactory.decodeStream(is);
                if (bitmap == null) {
                    Log.e(TAG, "Failed to decode logo image for channel ID " + mChannelId);
                    return null;
                }

                float scaleFactor = Math.min(1f, ((float) MAX_LOGO_IMAGE_SIZE) /
                        Math.max(bitmap.getWidth(), bitmap.getHeight()));
                if (scaleFactor < 1f) {
                    bitmap = Bitmap.createScaledBitmap(bitmap,
                            (int) (bitmap.getWidth() * scaleFactor),
                            (int) (bitmap.getHeight() * scaleFactor), false);
                }

                if (!mLogoPath.exists()) {
                    if (!mLogoPath.mkdirs()) {
                        Log.e(TAG, "Failed to create logo storage directory " + mLogoPath);
                        return null;
                    }
                }
                tempFile = File.createTempFile("img", null, mLogoPath);
                fos = new FileOutputStream(tempFile);
                bitmap.compress(Bitmap.CompressFormat.PNG, 100, fos);

                if (!tempFile.renameTo(getLogoFile(mChannelId))) {
                    Log.e(TAG, "Failed to rename logo for channel ID " + mChannelId);
                    return null;
                }
                tempFile = null;
                Uri channelUri = TvContract.buildChannelUri(mChannelId);
                getContext().getContentResolver().notifyChange(channelUri, null);
            } catch (IOException ioe) {
                Log.e(TAG, "Failed to write logo for channel ID " + mChannelId, ioe);
            } finally {
                IoUtils.closeQuietly(fos);
                IoUtils.closeQuietly(is);
                if (tempFile != null) {
                    tempFile.delete();
                }
            }
            return null;
        }
    }
}
