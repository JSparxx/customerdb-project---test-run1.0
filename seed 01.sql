-- SAMPLE DATA 

USE customerdb;

-- Customer
INSERT INTO Customer (FirstName, LastName, Email, Phone)
VALUES ('Nova','Edwards','SuperNova@example.com','813-555-0101');

-- Address
INSERT INTO Address (CustomerID, AddressLine1, City, State, ZipCode, Country, IsPrimary)
VALUES (
  (SELECT CustomerID FROM Customer WHERE Email='SuperNova@example.com'),
  '123 Main St', 'Tampa', 'FL', '33601', 'USA', TRUE
);

-- Employee
INSERT INTO Employee (FirstName, LastName, Email, Role)
VALUES ('Jay','Sparxx','sparxx.ops@example.com','CSR');

-- Product
INSERT INTO Product (Sku, ProductName, UnitPrice)
VALUES ('SKU-AX24','Axolotl Plush',15.99);

-- Lookups
INSERT IGNORE INTO OrderStatus (StatusName) VALUES ('Pending'), ('Paid');
INSERT IGNORE INTO OrderMethod (MethodName) VALUES ('Online'), ('In-Store');
INSERT IGNORE INTO PaymentMethod (MethodName) VALUES ('Card');

-- Order
INSERT INTO `Order` (CustomerID, EmployeeID, OrderStatusID, OrderMethodID, AddressID)
VALUES (
  (SELECT CustomerID FROM Customer WHERE Email='SuperNova@example.com'),
  (SELECT EmployeeID FROM Employee WHERE Email='sparxx.ops@example.com'),
  (SELECT OrderStatusID FROM OrderStatus WHERE StatusName='Paid'),
  (SELECT OrderMethodID FROM OrderMethod WHERE MethodName='Online'),
  (SELECT AddressID FROM Address WHERE IsPrimary=TRUE 
      AND CustomerID=(SELECT CustomerID FROM Customer WHERE Email='SuperNova@example.com'))
);

-- OrderItem
INSERT INTO OrderItem (OrderID, ProductID, Quantity, UnitPriceAtSale)
VALUES (
  (SELECT MAX(OrderID) FROM `Order`),
  (SELECT ProductID FROM Product WHERE Sku='SKU-AX24'),
  2, 15.99
);

-- Payment
INSERT INTO Payment (OrderID, PaymentMethodID, Amount)
VALUES (
  (SELECT MAX(OrderID) FROM `Order`),
  (SELECT PaymentMethodID FROM PaymentMethod WHERE MethodName='Card'),
  31.98
);

-- CustomerNote
INSERT INTO CustomerNote (CustomerID, CreatedByEmployeeID, NoteText)
VALUES (
  (SELECT CustomerID FROM Customer WHERE Email='SuperNova@example.com'),
  (SELECT EmployeeID FROM Employee WHERE Email='sparxx.ops@example.com'),
  'Call complete. Order confirmed.'
);