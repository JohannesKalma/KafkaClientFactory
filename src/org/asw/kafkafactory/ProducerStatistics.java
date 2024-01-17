package org.asw.kafkafactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class ProducerStatistics {

	int i=0;
	KafkaClientFactory cf;
	LocalDateTime startTime; 
	
	/**
	 * constructor
	 */
	public ProducerStatistics(KafkaClientFactory cf) {
		this.i=0;
		this.cf=cf;
		this.startTime=LocalDateTime.now();
		cf.printkv("Start", startTime.toString());
	}

  /**
   * increment the counter
   */
  public void incrI(){
    i++;
  }

  private int getI() {
  	return this.i;
  }
  
  public void printStats() {
  	LocalDateTime now = LocalDateTime.now();
  	Duration duration = Duration.between(this.startTime,now);

  	double avg=duration.toMillis()/i;
    double avgpersecond=1000/avg;
  	
    cf.printkv("End", now.toString());
    cf.print("==== Producer Statistics ====");

    if (duration.toSeconds() > 60) {
      cf.printkv("runtime (sec)",String.valueOf(duration.toSeconds()));
    } else {
    	cf.printkv("runtime (ms)",String.valueOf(duration.toMillis()));
    }
    
    cf.printkv("iterations", String.valueOf(getI()));
    cf.printkv("millisec per message (avg)", String.format("%.5f",avg));
    cf.printkv("messages per sec", String.format("%.2f", avgpersecond));
    cf.printkv("messages per minute", String.format("%.2f", avgpersecond*60));
  }
  
}
