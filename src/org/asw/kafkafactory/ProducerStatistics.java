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

  protected int getI() {
  	return this.i;
  }
  /**
   * Used for the publishFromRefCursor() method in the Producer
   */
  protected void printStats() {
  	LocalDateTime now = LocalDateTime.now();
  	Duration duration = Duration.between(this.startTime,now);
  	
  	if (i > 10) {
  		cf.printkv("Start", startTime.toString());
  	}
    cf.printkv("End", now.toString());

    Long durationSeconds = duration.toSeconds();
    
    if (durationSeconds > 60) {
      cf.printkv("runtime (sec)",String.valueOf(durationSeconds));
    } else {
    	cf.printkv("runtime (ms)",String.valueOf(duration.toMillis()));
    }
    
    cf.printkv("iterations", String.valueOf(getI()));
  }
  
}
