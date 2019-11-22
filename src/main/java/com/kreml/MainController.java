package com.kreml;

import com.kreml.kafka.AbstractKafkaConsumer;
import com.kreml.kafka.AvroKafka;
import com.kreml.kafka.StringKafka;
import javafx.application.Platform;
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
import javafx.scene.control.TextField;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import org.apache.commons.validator.routines.UrlValidator;

import java.util.List;

public class MainController implements RecordsProxy {

    @FXML
    public TextField topicNameField;
    @FXML
    public TextField brokerAddressField;
    @FXML
    public CheckBox shouldSeekToEndCheckBox;
    @FXML
    public Button startConsumer;
    @FXML
    public TextField schemaRegistryTextField;
    @FXML
    public CheckBox avroTopicCheckBox;
    @FXML
    private ListView<String> contentArea;

    private AbstractKafkaConsumer kafkaConsumer;
    private boolean isConsumerStarted;

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
    }

    @FXML
    public void startConsumer(MouseEvent mouseEvent) {
        if (!isConsumerStarted) {
            String topicName = topicNameField.getText();
            String brokerAddress = brokerAddressField.getText();
            if ((topicName != null && !topicName.isEmpty()) && (brokerAddress != null && !brokerAddress.isEmpty())) {
                if (selectConsumer()) return;
                kafkaConsumer
                        .setBrokerAddress(brokerAddress)
                        .setTopicName(topicName)
                        .setShouldSeekToEnd(shouldSeekToEndCheckBox.isSelected())
                        .runConsumer();
                isConsumerStarted = true;
                startConsumer.setText("Stop Consumer");
                shouldSeekToEndCheckBox.setDisable(true);
                avroTopicCheckBox.setDisable(true);
                ObservableList<String> items = contentArea.getItems();
                if (items != null) {
                    items.clear();
                }
            } else {
                showAlert("Please provide topic name and broker address.");
            }
        } else {
            startConsumer.setText("Start Consumer");
            shouldSeekToEndCheckBox.setDisable(false);
            avroTopicCheckBox.setDisable(false);
            kafkaConsumer.stopConsumer();
        }
    }

    /**
     * @return true if consumer was selected.
     */
    private boolean selectConsumer() {
        boolean stop = false;
        if (avroTopicCheckBox.isSelected()) {
            String schemaRegistryIp = schemaRegistryTextField.getText();
            if (schemaRegistryIp != null) {
                if (!schemaRegistryIp.startsWith("http")) {
                    schemaRegistryIp = String.format("http://%1$s", schemaRegistryIp);
                }
                if (UrlValidator.getInstance().isValid(schemaRegistryIp)) {
                    kafkaConsumer = new AvroKafka(this, schemaRegistryIp);
                } else {
                    showAlert("Please provide valid Schema Registry URL.");
                    stop = true;
                }
            }
        } else {
            kafkaConsumer = new StringKafka(this);
        }
        return stop;
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

    @Override
    public void records(List<String> records) {
        ObservableList<String> items = contentArea.getItems();
        items.addAll(records);
        Platform.runLater(() -> {
            contentArea.scrollTo(items.size());
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
        contentArea.getItems().clear();
        kafkaConsumer.resetCounter();
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
                    if (! cell.isEmpty()) {
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
            for (String s : selectionModel.getSelectedItems()){
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
}
