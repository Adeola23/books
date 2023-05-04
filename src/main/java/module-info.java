module com.example.assignment {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.sql;
    requires java.sql.rowset;


    opens com.example.assignmen_4 to javafx.fxml;
    exports com.example.assignmen_4;
}