package com.kreml;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;

public class StringTopicController implements DataProxy {

    @FXML
    public TextField topicNameField;
    @FXML
    public TextField brokerAddressField;
    @FXML
    public RadioButton shouldSeekToEnd;
    @FXML
    public Button startConsumer;
    @FXML
    private TextArea contentArea;
    private StringKafka stringKafka = new StringKafka(this);
    private boolean isConsumerStarted;

    public StringTopicController() {
    }

    @FXML
    public void startConsumer(MouseEvent mouseEvent) {
        if (isConsumerStarted = !isConsumerStarted) {
            String topicName = topicNameField.getText();
            String brokerAddress = brokerAddressField.getText();
            if ((topicName != null && !topicName.isEmpty()) && (brokerAddress != null && !brokerAddress.isEmpty())) {
                stringKafka
                        .setBrokerAddress(brokerAddress)
                        .setTopicName(topicName)
                        .setShouldSeekToEnd(shouldSeekToEnd.isSelected())
                        .runConsumer();
                startConsumer.setText("Stop Consumer");
                shouldSeekToEnd.setDisable(true);
            } else {
                showAlert("Please provide topic name and broker address.");
            }
        } else {
            startConsumer.setText("Stop Consumer");
            shouldSeekToEnd.setDisable(false);
            stringKafka.stopConsumer();
        }

    }

    public void showAlert(String text) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle("Information");
            alert.setHeaderText(null);
            alert.setContentText(text);
            alert.showAndWait();
        });
    }

    @Override
    public void data(String data) {
        contentArea.appendText(data);
    }
}
