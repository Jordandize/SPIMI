import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class SPIMIndex {
	
	public static final int BLOCK_SIZE      = 536870912; 		//2^29
	public static final int HASH_START_SIZE = 10922;
	public static final String OUTPUT_FILE  = "SPIMIndex";		//prefix

	private ArrayList<String> body;
	private Map<String, ArrayList<Integer>> index;
	
	private int  blockID    = 0;
	private long byteCount  = 0;
	private int  chunkSize  = 100; //n line to load from block
	
	public SPIMIndex(ArrayList<String> docs) {
		body = docs;
		index = new HashMap<>(HASH_START_SIZE);
		construction();
	}
	
	private void construction() {
		scanning();
		mergeBlocks();
	}
	
	/*
	 * Read tokens and add them
	 * to HashTable block
	*/
	private void scanning() {
		try {
			FileReader in;
			BufferedReader re;

			int docID = 0;
			for(String doc: body) {
				in = new FileReader(doc);
				re = new BufferedReader(in);
				
				docID++;
				String[] toks = {};
				String   line = "";
				while((line = re.readLine()) != null) {
					toks = line.split(" ");
					
					for(int i = 0; i < toks.length; i++) {
						String token = distinct(toks[i].toLowerCase());
						
						if(byteCount < BLOCK_SIZE) {
						//if memory is enough to work with current block
							if(!index.containsKey(token)) addToken(token,    docID);
							else						  updateToken(token, docID);
						} else {
						//else save current block and create new block
							releaseMemory();
						}
						//complete work with current token
					}
					//complete work with current line of tokens
				}
				//complete work with current document
				in.close();
				re.close();
			}
			//complete work with body
			releaseMemory();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void releaseMemory() {
		index = new TreeMap<>(index);   //sort HashTable
		saveBlock();
		newBlock();
	}
	
	private void saveBlock() {
		try {
			FileWriter out = new FileWriter(OUTPUT_FILE + '0' + blockID + ".txt");
			StringBuilder buil = new StringBuilder();
			
			for(String token: index.keySet()) {
				buil.setLength(0);
				buil.append(token);
				
				/*Write docIDs*/
				for(Integer docID: index.get(token)) {
					buil.append(" ");
					buil.append(docID);
				}
				
				buil.append("\n");
				out.write(buil.toString());
			}
			
			System.out.println("Block " + blockID + " has already saved");
			System.out.println("memory: " + byteCount + "\n");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void newBlock() {
		byteCount = 0;
		blockID++;
		index = new HashMap<>(HASH_START_SIZE);
	}

	private void mergeBlocks() {
		System.out.println("<Start to merge blocks>");
		index = new TreeMap<>();
		
		try {
			
			//Prepare blocks to merging
			String[] blockNames = parseBlocksNames(); 
			BufferedReader[] in = new BufferedReader[blockID];
			@SuppressWarnings("unchecked")
			TreeMap<String, ArrayList<Integer>> blockMaps[] = new TreeMap[blockID];
			
			for(int i = 0; i < blockID; i++) {
				in[i]        = new BufferedReader(new FileReader(blockNames[i]));
				blockMaps[i] = new TreeMap<String, ArrayList<Integer>>();
				blockMaps[i] = addChunkToBlock(in[i], blockMaps[i]);
			}
			//End prepare blocks to merging
			

			String top 			     = "";
			PriorityQueue<String> pq = null;
			String[] topTerms 		 = new String[blockID];
			
			while((topTerms = getTops(blockMaps)) != null) {
				
				pq = new PriorityQueue<>();
				for(int i = 0; i < blockID; i++)
					if(topTerms[i] != null) pq.add(topTerms[i]);				
				top = pq.peek();
				

				for(int i = 0; i < blockID; i++)
					if(blockMaps[i].size() < chunkSize * 1.5)
						blockMaps[i] = addChunkToBlock(in[i], blockMaps[i]);


				index.put(top, pullTerm(top, blockMaps));
				if(index.size() > chunkSize*100) writePortion();
				
			}
			
			writePortion(); //save traces
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void writePortion() throws IOException {
		BufferedWriter wr = new BufferedWriter(new FileWriter(OUTPUT_FILE + ".txt", true));
		StringBuilder buil = new StringBuilder();
		
		for(String token: index.keySet()) {
			buil.setLength(0);
			buil.append(token);
			
			//Write docIDs
			for(Integer docID: index.get(token)) {
				buil.append(" ");
				buil.append(docID);
			}
			
			buil.append("\n");
			wr.append(buil.toString());
		}
		
		index = new TreeMap<>();
		wr.close();
	}
	
	private ArrayList<Integer> pullTerm(String term, TreeMap<String, ArrayList<Integer>>[] maps) {
		ArrayList<Integer> posting = new ArrayList<>();
		
		for(int i = 0; i < maps.length; i++)
			if(maps[i].containsKey(term))
				posting.addAll(maps[i].remove((term)));
		
		Collections.sort(posting);
		return posting;
	}
	
	private TreeMap<String, ArrayList<Integer>> addChunkToBlock
		(BufferedReader source, TreeMap<String, ArrayList<Integer>> blockMap) throws IOException {
			int counter =  0;
			String line = "";
			
			while((line = source.readLine()) != null && counter < chunkSize) {
				String[] data = line.split(" ");
				ArrayList<Integer> posting = new ArrayList<>();
				
				for(int i = 1; i < data.length; i++) {
					posting.add(Integer.parseInt(data[i]));
				}
				
				blockMap.put(data[0], posting);
			}
			
			return blockMap;
	}
	
	private String[] getTops(TreeMap<String, ArrayList<Integer>>[] blockMaps) throws IOException {
		String[] terms = new String[blockID];
		int isEmpty = 0;

		for(int i = 0; i < blockID; i++) {
			if(blockMaps[i].size() != 0) terms[i] = blockMaps[i].firstKey();
			else						 isEmpty++;
		}
		
		if(isEmpty == blockID) return null;
		return terms;
	}
	
	private String[] parseBlocksNames() {
		String[] blocks = new String[blockID];
		
		for(int i = 0; i < blockID; i++) {
			blocks[i] = OUTPUT_FILE + '0' + i + ".txt";
		}
		
		return blocks;
	}
	
	private void addToken(String term, int docID) {
		ArrayList<Integer> list = new ArrayList<>();
		list.add(docID);
		index.put(term, list);
		/*
		 * new String()  +  24
		 * new char[4]   + ~20
		 * new Integer() +  16
		*/
		byteCount += 60;
	}
	
	private void updateToken(String term, int docID) {
		ArrayList<Integer> list = index.get(term);
		
		//Doesn't double docIDs Value for same term
		if(list.get(list.size()-1) != docID) {
			list.add(docID);
			index.put(term, list);
			//new Integer() + 16
			byteCount += 16;
		}
	}
	
	public static String distinct(String token) {
		StringBuilder buil = new StringBuilder();
		
		for(int j = 0; j < token.length(); j++) {
			char ch = token.charAt(j);
			if(Character.isLetterOrDigit(ch))
				buil.append(ch);
		}
		
		return buil.toString();
	}

	public static void main(String[] args) {
		 long stime = System.currentTimeMillis();
			
			File[] files = new File("texts").listFiles();
			ArrayList<String> documents = new ArrayList<String>(files.length);
			
			for(int i = 0; i < files.length; i++)
				documents.add("texts//" + files[i].getName());
			Collections.sort(documents);
			
			SPIMIndex sp = new SPIMIndex(documents);
			
			long etime = System.currentTimeMillis();
			System.out.println("Time in ms: " + (etime - stime));
			System.out.println("Blocks amount: " + sp.blockID);

			System.out.println("OK");
	}

}
