package com.kreml;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

public class Randomizer {

   private Random random = new Random();
   private Set<Integer> set = new LinkedHashSet<>();
   private int min;
   private int capability;

   public Randomizer(int min, int max) {
      this.min = min;
      capability = max - min + 1;
   }

   public int randomize() {
      int randomNumber = random.nextInt(capability) + min;
      if (set.size() == capability) {
         set.clear();
      }
      if (!set.add(randomNumber)){
         return randomize();
      }
      return randomNumber;
   }

   public int getCapability() {
      return capability;
   }
}
