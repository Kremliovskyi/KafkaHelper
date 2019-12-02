package com.kreml.kafka;

public interface OnCancelListener {

    void setOnCancelled(Runnable runnable);
}
