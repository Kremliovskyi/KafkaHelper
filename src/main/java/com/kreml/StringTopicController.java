package com.kreml;

import com.kreml.kafka.StringKafka;
import javafx.application.Platform;
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

public class StringTopicController implements DataProxy {

    @FXML
    public TextField topicNameField;
    @FXML
    public TextField brokerAddressField;
    @FXML
    public CheckBox shouldSeekToEndCheckBox;
    @FXML
    public Button startConsumer;
    @FXML
    private ListView<String> contentArea;
    private StringKafka stringKafka = new StringKafka(this);
    private boolean isConsumerStarted;

    public StringTopicController() {
    }

    @FXML
    public void initialize() {
        initContentArea();
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
                        .setShouldSeekToEnd(shouldSeekToEndCheckBox.isSelected())
                        .runConsumer();
                startConsumer.setText("Stop Consumer");
                shouldSeekToEndCheckBox.setDisable(true);
                contentArea.getItems().clear();
            } else {
                showAlert("Please provide topic name and broker address.");
            }
        } else {
            startConsumer.setText("Start Consumer");
            shouldSeekToEndCheckBox.setDisable(false);
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
        contentArea.getItems().add(data);
    }

    @FXML
    public void selectAll(MouseEvent mouseEvent) {
        contentArea.getSelectionModel().selectAll();
    }

    @FXML
    public void deSelectAll(MouseEvent mouseEvent) {
        contentArea.getSelectionModel().clearSelection();
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
