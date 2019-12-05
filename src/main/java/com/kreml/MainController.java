package com.kreml;

import com.kreml.kafka.AbstractKafkaConsumerGroup;
import com.kreml.kafka.AvroKafkaConsumerGroup;
import com.kreml.kafka.StringKafkaConsumerGroup;
import javafx.application.Platform;
import javafx.beans.property.ListProperty;
import javafx.beans.property.SimpleListProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.MenuItem;
import javafx.scene.control.MultipleSelectionModel;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.Spinner;
import javafx.scene.control.TextField;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import org.apache.commons.validator.routines.UrlValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class MainController {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @FXML
    public TextField topicNameField;
    @FXML
    public TextField brokerAddressesField;
    @FXML
    public CheckBox shouldSeekToEndCheckBox;
    @FXML
    public Button startConsumers;
    @FXML
    public TextField schemaRegistryTextField;
    @FXML
    public CheckBox avroTopicCheckBox;
    @FXML
    public Button seeLogs;
    @FXML
    public Spinner consumersSpinner;
    @FXML
    private ListView<String> contentArea;

    private AbstractKafkaConsumerGroup<?> kafkaConsumer;
    private boolean isConsumerStarted;
    private ObservableList<String> observableList = FXCollections.observableList(new ArrayList<>());
    private ListProperty<String> stringListProperty = new SimpleListProperty<>(observableList);

    public MainController() {
    }

    @FXML
    public void initialize() {
        initContentArea();
        schemaRegistryTextField.managedProperty().bind(schemaRegistryTextField.visibleProperty());
        schemaRegistryTextField.setVisible(false);
        schemaRegistryTextField.visibleProperty().bind(avroTopicCheckBox.selectedProperty());
        avroTopicCheckBox.selectedProperty().addListener((observable, oldValue, newValue) -> {
            schemaRegistryTextField.clear();
        });
        contentArea.itemsProperty().bind(stringListProperty);
        Platform.runLater(() -> contentArea.requestFocus());
    }

    @FXML
    public void startConsumers(MouseEvent mouseEvent) {
        if (!isConsumerStarted) {
            onConsumerStart();
        } else {
            onConsumerStop();
        }
    }

    private void onConsumerStart() {
        String topicName = topicNameField.getText();
        String brokerAddresses = brokerAddressesField.getText();
        if ((topicName != null && !topicName.isEmpty()) && validateBrokersAddresses(brokerAddresses)) {
            if (selectConsumer()) return;
            kafkaConsumer
                    .setBrokerAddresses(brokerAddresses)
                    .setTopicName(topicName)
                    .setShouldSeekToEnd(shouldSeekToEndCheckBox.isSelected())
                    .setConsumersCount((Integer) consumersSpinner.getValue())
                    .runConsumer();
            kafkaConsumer.setOnCancelled(() -> {
                startConsumers.setDisable(false);
                startConsumers.setText("Start Consumers");
                shouldSeekToEndCheckBox.setDisable(false);
                avroTopicCheckBox.setDisable(false);
                brokerAddressesField.setDisable(false);
                topicNameField.setDisable(false);
                schemaRegistryTextField.setDisable(false);
            });
            isConsumerStarted = true;
            startConsumers.setText("Stop Consumers");
            brokerAddressesField.setDisable(true);
            topicNameField.setDisable(true);
            shouldSeekToEndCheckBox.setDisable(true);
            avroTopicCheckBox.setDisable(true);
            schemaRegistryTextField.setDisable(true);
            ObservableList<String> items = contentArea.getItems();
            if (items != null) {
                items.clear();
            }
        } else {
            showAlert("Please provide topic name and bootstrap servers separated with , or ;");
        }
    }

    private void onConsumerStop() {
        startConsumers.setDisable(true);
        kafkaConsumer.stopConsumer();
        isConsumerStarted = false;
    }

    /**
     * @return true if consumer was selected.
     */
    private boolean selectConsumer() {
        boolean stop = false;
        if (avroTopicCheckBox.isSelected()) {
            String schemaRegistryIp = schemaRegistryTextField.getText();
            if (validateURL(schemaRegistryIp)) {
                kafkaConsumer = new AvroKafkaConsumerGroup(observableList, schemaRegistryIp);
            } else {
                showAlert("Please provide valid Schema Registry URL.");
                stop = true;
            }
        } else {
            kafkaConsumer = new StringKafkaConsumerGroup(observableList);
        }
        return stop;
    }

    private boolean validateURL(String urlString) {
        boolean result = false;
        if (urlString != null) {
            if (!urlString.startsWith("http")) {
                urlString = String.format("http://%1$s", urlString);
            }
            result = UrlValidator.getInstance().isValid(urlString);
        }
        return result;
    }

    private boolean validateBrokersAddresses(String addresses){
        boolean result = false;
        if (addresses != null && !addresses.isEmpty()) {
            String[] addressesArray = addresses.split("[,;]+");
            result = Arrays.stream(addressesArray).allMatch(s -> validateURL(s.trim()));
        }
        return result;
    }

    private void showAlert(String text) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle("Information");
            alert.setHeaderText(null);
            alert.setContentText(text);
            alert.showAndWait();
        });
    }

    @FXML
    public void selectAll(MouseEvent mouseEvent) {
        contentArea.getSelectionModel().selectAll();
    }

    @FXML
    public void deSelectAll(MouseEvent mouseEvent) {
        contentArea.getSelectionModel().clearSelection();
    }

    @FXML
    public void clear(MouseEvent mouseEvent) {
        kafkaConsumer.resetList();
    }

    private void initContentArea() {
        MultipleSelectionModel<String> selectionModel = contentArea.getSelectionModel();
        selectionModel.setSelectionMode(SelectionMode.MULTIPLE);
        contentArea.setCellFactory(param -> {
            ListCell<String> cell = new ListCell<>();
            cell.textProperty().bind(cell.itemProperty());
            cell.addEventFilter(MouseEvent.MOUSE_PRESSED, event -> {
                if (event.getButton().equals(MouseButton.PRIMARY)) {
                    contentArea.requestFocus();
                    if (!cell.isEmpty()) {
                        int index = cell.getIndex();
                        if (selectionModel.getSelectedIndices().contains(index)) {
                            selectionModel.clearSelection(index);
                        } else {
                            selectionModel.select(index);
                        }
                        event.consume();
                    }
                }
            });
            return cell;
        });
        MenuItem item = new MenuItem("Copy");
        item.setOnAction(event -> {
            StringBuilder clipboardString = new StringBuilder();
            for (String s : selectionModel.getSelectedItems()) {
                clipboardString.append(s).append("\n");
            }
            final ClipboardContent content = new ClipboardContent();
            content.putString(clipboardString.toString());
            Clipboard.getSystemClipboard().setContent(content);
        });
        ContextMenu menu = new ContextMenu();
        menu.getItems().add(item);
        contentArea.setContextMenu(menu);
    }

    @FXML
    public void openLogs(MouseEvent mouseEvent) {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File logFile = new File(tempDir, "/logs/kafka-debug.log");
        if (logFile.exists()) {
            Desktop dt = Desktop.getDesktop();
            try {
                dt.open(logFile);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
