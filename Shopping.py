import psycopg2
def load_items_from_db():
    items = []
    try:
        conn = psycopg2.connect(
            host="localhost",  # or your server IP
            database="testdb",  # name of your PostgreSQL database
            user="admin",  # your PostgreSQL username
            password="admin123"  # your PostgreSQL password
        )

        # Step 2: Create a cursor object
        cur = conn.cursor()

        cur.execute("SELECT id, name, quantity, price FROM items order by id asc")
        rows = cur.fetchall()

        for row in rows:
            # Each row: (id, name, quantity, price)
            items.append([row[0], row[1], row[2], row[3]])

        cur.close()
        conn.close()

    except Exception as e:
        print("Error loading items:", e)

    return items

def update_quantity_in_db(item_id, new_quantity):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="testdb",
            user="admin",
            password="admin123"
        )
        cur = conn.cursor()
        cur.execute(
            "UPDATE items SET quantity = %s WHERE id = %s",
            (new_quantity, item_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        #
    except Exception as e:
        print("Error updating item quantity:", e)
def main():

    running = True
    shopping = True
    while running:
        item1 = [0, "Burgers", 10, 6.99]
        item2 = [1, "Fries", 15, 8.99]
        item3 = [2, "Pop", 11, 2.99]
        items = load_items_from_db()
        cart = {}
        while shopping:

            #items = {"Burgers": 10, "Fries": 15, "Pop": 10}
            index = 0
            print("%5s | %10s | %10s |%10s" % ("Sr.No" , "Item", "Quantity", "Price"))
            print("%5s | %10s | %10s |%10s" % ("-----", "----------", "---------", "-------------"))
            for item in items:
                print(("%5s | %10s | %10d | %10.2f" % (item[0], item[1], item[2], item[3])))
            whatToBuy = int(input("What do you want to buy? ")) - 1
            quantity = int(input("How many %s do you want to buy?" % items[whatToBuy][1]))
            while items[whatToBuy][2] < quantity:
                print("Available quantity of %s is %d" % (items[whatToBuy][1], items[whatToBuy][2]))
                quantity = 0
                quantity = int(input("How many %s do you want to buy?" % items[whatToBuy][1]))

            cart[whatToBuy] = quantity
            items[whatToBuy][2] = items[whatToBuy][2] - quantity
            update_quantity_in_db(items[whatToBuy][0], items[whatToBuy][2])

            contShopping = input("Do you want to continue shopping? [Y/N]")
            if contShopping == "N" or contShopping == "n":
                shopping = False
        name = input("What is your name?")
        address = input("What is your address?")
        distance = int(input("What is your distance from the store?"))
        deliveryCharge = 0
        if distance <= 15:
            deliveryCharge = 50
            print("Delivery charge is %d" % deliveryCharge)
        if distance <= 50 and distance > 15:
            deliveryCharge = 100
            print("Delivery charge is %d" % deliveryCharge)
        total = 0.00

        print("Items purchased:")
        print("%5s | %10s | %10s |%10s" % ("SKU", "Item", "Quantity", "Price"))
        print("%5s | %10s | %10s |%10s" % ("-----", "----------", "---------", "-------------"))
        for cartItem in cart:
            price = items[cartItem][3]
            total += price * cart[cartItem]
            print(("%5s | %10s | %10d | %10.2f" % (cartItem + 1, items[cartItem][1], cart[cartItem], price * cart[cartItem])))

        total = total + deliveryCharge
        print("Price is %.2f" % total)
        running = False

if __name__ == "__main__":
    main()