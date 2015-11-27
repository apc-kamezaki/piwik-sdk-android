/*
 * Android SDK for Piwik
 *
 * @link https://github.com/piwik/piwik-android-sdk
 * @license https://github.com/piwik/piwik-sdk-android/blob/master/LICENSE BSD-3 Clause
 */

package org.piwik.sdk;

import android.content.Context;
import android.os.Process;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HTTP;
import org.json.JSONObject;
import org.piwik.sdk.tools.Logy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Sends json POST request to tracking url http://piwik.example.com/piwik.php with body
 * <p/>
 * {
 * "requests": [
 * "?idsite=1&url=http://example.org&action_name=Test bulk log Pageview&rec=1",
 * "?idsite=1&url=http://example.net/test.htm&action_name=Another bul k page view&rec=1"
 * ],
 * "token_auth": "33dc3f2536d3025974cccb4b4d2d98f4"
 * }
 */
@SuppressWarnings("deprecation")
public class Dispatcher {
    private static final String LOGGER_TAG = Piwik.LOGGER_PREFIX + "Dispatcher";
    private static final String EVENTS_FILE = "piwik_sdk_events.txt";
    private final BlockingQueue<String> mDispatchQueue = new LinkedBlockingQueue<>();
    private final Object mThreadControl = new Object();
    private final Semaphore mSleepToken = new Semaphore(0);
    private final Piwik mPiwik;
    private final URL mApiUrl;
    private final String mAuthToken;

    private List<HttpRequestBase> mDryRunOutput = Collections.synchronizedList(new ArrayList<HttpRequestBase>());

    private volatile int mTimeOut = 5 * 1000; // 5s
    private volatile boolean mRunning = false;
    private volatile boolean mWriteFileOnError = false;

    private volatile long mDispatchInterval = 120 * 1000; // 120s

    public Dispatcher(Piwik piwik, URL apiUrl, String authToken) {
        mPiwik = piwik;
        mApiUrl = apiUrl;
        mAuthToken = authToken;

        List<String> remainEvents = restoreEventsFromFile();
        if (remainEvents.size() > 0)
            mDispatchQueue.addAll(remainEvents);
    }

    /**
     * Connection timeout in miliseconds
     *
     * @return
     */
    public int getTimeOut() {
        return mTimeOut;
    }

    public void setTimeOut(int timeOut) {
        mTimeOut = timeOut;
    }

    public void setDispatchInterval(long dispatchInterval) {
        mDispatchInterval = dispatchInterval;
        if (mDispatchInterval != -1)
            launch();
    }

    public long getDispatchInterval() {
        return mDispatchInterval;
    }

    protected void setWriteFileOnError(boolean needOutputFile) {
        mWriteFileOnError = needOutputFile;
    }

    private boolean launch() {
        synchronized (mThreadControl) {
            if (!mRunning) {
                mRunning = true;
                new Thread(mLoop).start();
                return true;
            }
        }
        return false;
    }

    /**
     * Starts the dispatcher for one cycle if it is currently not working.
     * If the dispatcher is working it will skip the dispatch intervall once.
     */
    public void forceDispatch() {
        if (!launch()) {
            mSleepToken.release();
        }
    }

    public void submit(String query) {
        mDispatchQueue.add(query);
        if (mDispatchInterval != -1)
            launch();
    }

    private Runnable mLoop = new Runnable() {
        @Override
        public void run() {
            android.os.Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
            while (mRunning) {
                try {
                    // Either we wait the interval or forceDispatch() granted us one free pass
                    mSleepToken.tryAcquire(mDispatchInterval, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int count = 0;
                List<String> availableEvents = new ArrayList<>();
                mDispatchQueue.drainTo(availableEvents);
                Logy.d(LOGGER_TAG, "Drained " + availableEvents.size() + " events.");
                TrackerBulkURLWrapper wrapper = new TrackerBulkURLWrapper(mApiUrl, availableEvents, mAuthToken);
                Iterator<TrackerBulkURLWrapper.Page> pageIterator = wrapper.iterator();
                TrackerBulkURLWrapper.Page page = null;
                boolean success = true;
                while (pageIterator.hasNext()) {
                    page = pageIterator.next();

                    // use doGET when only event on current page
                    if (page.elementsCount() > 1) {
                        if (!doPost(wrapper.getApiUrl(), wrapper.getEvents(page))) {
                            success = false;
                            break;
                        }
                        count += page.elementsCount();
                    } else {
                        if (!doGet(wrapper.getEventUrl(page))) {
                            success = false;
                            break;
                        }
                        count += 1;
                    }
                }
                if (!success) {
                    do {
                        mDispatchQueue.addAll(wrapper.getEventsByList(page));
                    } while ((page = pageIterator.next()) != null);
                    if (mWriteFileOnError) {
                        List<String> remainEvents = new ArrayList<>();
                        mDispatchQueue.drainTo(remainEvents);
                        saveEventsToFile(remainEvents);
                    }
                }
                Logy.d(LOGGER_TAG, "Dispatched " + count + " events.");
                synchronized (mThreadControl) {
                    // We may be done or this was a forced dispatch
                    if (mDispatchQueue.isEmpty() || mDispatchInterval < 0 || !success) {
                        mRunning = false;
                        break;
                    }
                }
            }
        }
    };

    private boolean doGet(String trackingEndPointUrl) {
        if (trackingEndPointUrl == null)
            return false;
        HttpGet get = new HttpGet(trackingEndPointUrl);
        return doRequest(get);
    }

    private boolean doPost(URL url, JSONObject json) {
        if (url == null || json == null)
            return false;

        String jsonBody = json.toString();
        try {
            HttpPost post = new HttpPost(url.toURI());
            StringEntity se = new StringEntity(jsonBody);
            se.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            post.setEntity(se);

            return doRequest(post);
        } catch (URISyntaxException e) {
            Logy.w(LOGGER_TAG, String.format("URI Syntax Error %s", url.toString()), e);
        } catch (UnsupportedEncodingException e) {
            Logy.w(LOGGER_TAG, String.format("Unsupported Encoding %s", jsonBody), e);
        }
        return false;
    }

    private boolean doRequest(HttpRequestBase requestBase) {
        HttpClient client = new DefaultHttpClient();
        HttpConnectionParams.setConnectionTimeout(client.getParams(), mTimeOut);
        HttpResponse response;

        if (mPiwik.isDryRun()) {
            Logy.d(LOGGER_TAG, "DryRun, stored HttpRequest, now " + mDryRunOutput.size());
            mDryRunOutput.add(requestBase);
        } else {
            if (!mDryRunOutput.isEmpty())
                mDryRunOutput.clear();
            try {
                response = client.execute(requestBase);
                int statusCode = response.getStatusLine().getStatusCode();
                Logy.d(LOGGER_TAG, String.format("status code %s", statusCode));
                return statusCode == HttpStatus.SC_NO_CONTENT || statusCode == HttpStatus.SC_OK;
            } catch (Exception e) {
                Logy.w(LOGGER_TAG, "Cannot send request", e);
            }
        }
        return false;
    }

    /**
     * http://stackoverflow.com/q/4737841
     *
     * @param param raw data
     * @return encoded string
     */
    public static String urlEncodeUTF8(String param) {
        try {
            return URLEncoder.encode(param, "UTF-8").replaceAll("\\+", "%20");
        } catch (UnsupportedEncodingException e) {
            Logy.w(LOGGER_TAG, String.format("Cannot encode %s", param), e);
            return "";
        } catch (NullPointerException e) {
            return "";
        }
    }

    /**
     * For bulk tracking purposes
     *
     * @param map query map
     * @return String "?idsite=1&url=http://example.org&action_name=Test bulk log view&rec=1"
     */
    public static String urlEncodeUTF8(Map<String, String> map) {
        StringBuilder sb = new StringBuilder(100);
        sb.append('?');
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(urlEncodeUTF8(entry.getKey()));
            sb.append('=');
            sb.append(urlEncodeUTF8(entry.getValue()));
            sb.append('&');
        }

        return sb.substring(0, sb.length() - 1);
    }

    public List<HttpRequestBase> getDryRunOutput() {
        return mDryRunOutput;
    }

    private List<String> restoreEventsFromFile() {
        File file = mPiwik.getContext().getFileStreamPath(EVENTS_FILE);
        Logy.e(LOGGER_TAG, "file name " + file.getAbsolutePath());
        if (!file.exists()) {
            return Collections.EMPTY_LIST;
        }
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = mPiwik.getContext().openFileInput(EVENTS_FILE);
            br = new BufferedReader(new FileReader(file));
            String line;
            List<String> events = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                if (line.length() > 0) {
                    events.add(line);
                }
            }

            return events;
        } catch (IOException e) {
            Logy.w(LOGGER_TAG, "file read error", e);
            return Collections.EMPTY_LIST;
        } finally {
            closeResource(br);
            closeResource(is);
            file.delete();
        }
    }

    private void saveEventsToFile(List<String> events) {
        if (events == null || events.size() < 1)
            return;
        File file = mPiwik.getContext().getFileStreamPath(EVENTS_FILE);
        if (file.exists())
            file.delete();
        OutputStream stream = null;
        BufferedWriter bw = null;
        try {
            stream = mPiwik.getContext().openFileOutput(EVENTS_FILE, Context.MODE_PRIVATE);
            bw = new BufferedWriter(new OutputStreamWriter(stream));
            for (String event : events) {
                bw.write(event);
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {
            Logy.w(LOGGER_TAG, "file write error", e);
            file.delete();
        } finally {
            closeResource(bw);
            closeResource(stream);
        }

    }

    private void closeResource(Closeable closable) {
        try {
            if (closable != null) {
                closable.close();
            }
        } catch (Exception ignore) {

        }
    }
}
