package zhangblue.test;

import java.util.ArrayList;
import java.util.List;

public class TestString {

  public void lalala(String... cc) {

  }


  public static void main(String[] args) {

    List<String> list = new ArrayList<>();
    String[] aa = {};

    new TestString().lalala(list.toArray(new String[]{}));
  }

}
