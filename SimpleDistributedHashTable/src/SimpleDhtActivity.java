package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SimpleDhtActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        findViewById(R.id.button1).setOnClickListener(
                new OnLDumpClickListener(tv, getContentResolver()));
        findViewById(R.id.button2).setOnClickListener(
                new OnGDumpClickListener(tv, getContentResolver()));

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
