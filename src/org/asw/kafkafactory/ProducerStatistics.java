package org.asw.kafkafactory;

import java.time.Duration;
import java.time.LocalDateTime;
/**
 * DTO used in Producer to generate statistics for a publisher based on a ref cursor<br> 
 * That might produce millions of messages in short time
 *
 */
public class ProducerStatistics {

	int i=0;
	KafkaClientFactory cf;
	LocalDateTime startTime; 
	
/**
 * constructor
 * @param cf KafkaClientFactory
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
  /**
   * Used for the publishFromRefCursor() method in the Producer
   */
  public void printStats() {
  	LocalDateTime now = LocalDateTime.now();
  	Duration duration = Duration.between(this.startTime,now);

  	double avg = -1;
  	double avgpersecond = -1;
  	try {
  	 avg=duration.toMillis()/i;
  	} catch (Exception e) {}
  	
  	try {
      avgpersecond=1000/avg;
  	} catch (Exception e) {}
  	
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
