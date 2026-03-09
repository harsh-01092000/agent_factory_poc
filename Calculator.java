public class Calculator {

    public static int add(int a, int b) {
        return a + b;
    }

    public static int subtract(int a, int b) {
        return a - b;
    }

    public static int multiply(int a, int b) {
        return a * b;
    }

    public static double divide(int a, int b) {
        if (b != 0) {
            return (double) a / b;
        } else {
            throw new ArithmeticException("Cannot divide by zero");
        }
    }

    public static void main(String[] args) {
        System.out.println("Add: " + add(5, 3));
        System.out.println("Subtract: " + subtract(5, 3));
        System.out.println("Multiply: " + multiply(5, 3));
        System.out.println("Divide: " + divide(5, 3));
    }
}
