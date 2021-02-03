package ridecleansing

import common.sources.TaxiRideGenerator
import common.utils.{ExerciseBase, GeoUtils}
import common.utils.ExerciseBase._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @author hzh
 * @date 2021/2/2 下午2:53
 * @version 1.0
 */
object RideCleansingSolution {

  def main(args: Array[String]) {
    // web ui config
    val conf = new Configuration
    conf.setInteger(RestOptions.PORT, 8050)

    // set up the execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(ExerciseBase.parallelism)

    // get the taxi ride data stream
    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val filteredRides = rides
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    // print the filtered stream
    printOrTest(filteredRides)

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}
