package invertedindex;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;


/**
 * A Comparator used for ordering Text objects in descending or based on the Text objects length, but ascending alphabetically within Text objects of the same length.
 * @author elr17
 *
 */
public class ArticleComparator implements RawComparator<Text> {

	//variable to store the integer result of a comparison
	int result;
	
	/**
	 * This method compares the lengths of two Text objects
	 * @param o1 One of the Text object to be compared 
	 * @param o2 One of the Text object to be compared 
	 * @return An integer result of the comparison
	 */
	@Override
	public int compare(Text o1, Text o2) {
		
		result = Integer.compare(o2.getLength(), o1.getLength());
		
		if (result == 0) {
			return o1.compareTo(o2);
		}
		
		return result;
	}

	
	/**
	 * This method compares the length of the byte representation of two Text Objects
	 * @param arg0 The first byte array
	 * @param arg1 The position index in arg0
	 * @param arg2 The length of the object in arg0
	 * @param arg3 The second byte array
	 * @param arg4 The position index in arg3
	 * @param arg5 The length of the object in arg3
	 * @return An integer result of the comparison
	 */
	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		
		byte[] b1 = new byte[arg2];
		byte[] b2 = new byte[arg5];
		
		System.arraycopy(arg0, arg1, b1, 0, arg2);
		System.arraycopy(arg3, arg4, b2, 0, arg5);
		
		Text t1 = new Text(new String(b1));
		Text t2 = new Text(new String(b2));
		
		return compare(t1, t2);
	}
}