-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)

SELECT
	cust.company_name AS customer,
	CONCAT(empl.first_name, ' ', empl.last_name) AS employee
FROM customers AS cust
JOIN orders AS ord
	USING(customer_id)
JOIN employees AS empl
	USING(employee_id)
JOIN shippers AS ship
	ON ord.ship_via = ship.shipper_id
WHERE
	cust.city = 'London'
	AND
	empl.city = 'London'
	AND
	ship.company_name = 'United Package';

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.

SELECT
	prod.product_name,
	prod.units_in_stock,
	sup.contact_name,
	sup.phone
FROM products AS prod
JOIN suppliers AS sup
	USING(supplier_id)
WHERE
	prod.discontinued = 0
	AND
	prod.units_in_stock < 25
	AND
	prod.category_id IN (
		SELECT category_id
		FROM categories
		WHERE
			category_name = 'Dairy Products'
			OR
			category_name = 'Condiments'
	)
ORDER BY prod.units_in_stock

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа

SELECT company_name
FROM customers
WHERE customer_id NOT IN (
	SELECT customer_id
	FROM orders
	)

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
