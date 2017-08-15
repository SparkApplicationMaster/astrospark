/*******************************************************************************
 * Copyright (c) 2010 Haifeng Li
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package correlate;

/**
 * A KD-tree (short for k-dimensional tree) is a space-partitioning dataset
 * structure for organizing points in a k-dimensional space. KD-trees are
 * a useful dataset structure for nearest neighbor searches. The kd-tree is a
 * binary tree in which every node is a k-dimensional point. Every non-leaf
 * node generates a splitting hyperplane that divides the space into two
 * subspaces. Points left to the hyperplane represent the left sub-tree of
 * that node and the points right to the hyperplane by the right sub-tree.
 * The hyperplane direction is chosen in the following way: every node split
 * to sub-trees is associated with one of the k-dimensions, such that the
 * hyperplane is perpendicular to that dimension vector. So, for example, if
 * for a particular split the "x" axis is chosen, all points in the subtree
 * with a smaller "x" value than the node will appear in the left subtree and
 * all points with larger "x" value will be in the right sub tree.
 * <p>
 * KD-trees are not suitable for efficiently finding the nearest neighbor
 * in high dimensional spaces. As a general rule, if the dimensionality is D,
 * then number of points in the dataset, N, should be N &gt;&gt; 2<sup>D</sup>.
 * Otherwise, when kd-trees are used with high-dimensional dataset, most of the
 * points in the tree will be evaluated and the efficiency is no better than
 * exhaustive search, and approximate nearest-neighbor methods should be used
 * instead.
 * <p>
 * By default, the query object (reference equality) is excluded from the neighborhood.
 * You may change this behavior with <code>setIdenticalExcluded</code>. Note that
 * you may observe weird behavior with String objects. JVM will pool the string literal
 * objects. So the below variables
 * <code>
 *     String a = "ABC";
 *     String b = "ABC";
 *     String c = "AB" + "C";
 * </code>
 * are actually equal in reference test <code>a == b == c</code>. With toy data that you
 * type explicitly in the code, this will cause problems. Fortunately, the data would be
 * read from secondary storage in production.
 * </p>
 *
 * @param <E> the type of data objects in the tree.
 *
 * @author Haifeng Li
 */
public class CountTree {

    /**
     * The root in the KD-tree.
     */
    class Node {

        /**
         * Number of dataset stored in this node.
         */
        int count;
        /**
         * The smallest point index stored in this node.
         */
        int index;
        /**
         * The index of coordinate used to split this node.
         */
        int split;
        /**
         * The cutoff used to split the specific coordinate.
         */
        double cutoff;
        /**
         * The child node which values of split coordinate is less than the cutoff value.
         */
        Node lower;
        /**
         * The child node which values of split coordinate is greater than or equal to the cutoff value.
         */
        Node upper;

        /**
         * If the node is a leaf node.
         */
        boolean isLeaf() {
            return lower == null && upper == null;
        }
    }
    /**
     * The keys of data objects.
     */
    private double[][] keys;
    /**
     * The data objects.
     */
    private long[] data;
    /**
     * The root node of KD-Tree.
     */
    private Node root;
    /**
     * The index of objects in each nodes.
     */
    private int[] index;

    /**
     * Constructor.
     * @param key the keys of data objects.
     * @param data the data objects.
     */
    public CountTree(double[][] key, long[] data) {
        if (key.length != data.length) {
            throw new IllegalArgumentException("The array size of keys and data are different.");
        }

        this.keys = key;
        this.data = data;

        int n = key.length;
        index = new int[n];
        for (int i = 0; i < n; i++) {
            index[i] = i;
        }

        // Build the tree
        root = buildNode(0, n);
    }

    @Override
    public String toString() {
        return "KD-Tree";
    }

    /**
     * Build a k-d tree from the given set of dataset.
     */
    private Node buildNode(int begin, int end) {
        int d = keys[0].length;

        // Allocate the node
        Node node = new Node();

        // Fill in basic info
        node.count = end - begin;
        node.index = begin;

        // Calculate the bounding box
        double[] lowerBound = new double[d];
        double[] upperBound = new double[d];

        for (int i = 0; i < d; i++) {
            lowerBound[i] = keys[index[begin]][i];
            upperBound[i] = keys[index[begin]][i];
        }

        for (int i = begin + 1; i < end; i++) {
            for (int j = 0; j < d; j++) {
                double c = keys[index[i]][j];
                if (lowerBound[j] > c) {
                    lowerBound[j] = c;
                }
                if (upperBound[j] < c) {
                    upperBound[j] = c;
                }
            }
        }

        // Calculate bounding box stats
        double maxRadius = -1;
        for (int i = 0; i < d; i++) {
            double radius = (upperBound[i] - lowerBound[i]) / 2;
            if (radius > maxRadius) {
                maxRadius = radius;
                node.split = i;
                node.cutoff = (upperBound[i] + lowerBound[i]) / 2;
            }
        }

        // If the max spread is 0, make this a leaf node
        if (maxRadius == 0) {
            node.lower = node.upper = null;
            return node;
        }

        // Partition the dataset around the midpoint in this dimension. The
        // partitioning is done in-place by iterating from left-to-right and
        // right-to-left in the same way that partioning is done in quicksort.
        int i1 = begin, i2 = end - 1, size = 0;
        while (i1 <= i2) {
            boolean i1Good = (keys[index[i1]][node.split] < node.cutoff);
            boolean i2Good = (keys[index[i2]][node.split] >= node.cutoff);

            if (!i1Good && !i2Good) {
                int temp = index[i1];
                index[i1] = index[i2];
                index[i2] = temp;
                i1Good = i2Good = true;
            }

            if (i1Good) {
                i1++;
                size++;
            }

            if (i2Good) {
                i2--;
            }
        }

        // Create the child nodes
        node.lower = buildNode(begin, begin + size);
        node.upper = buildNode(begin + size, end);

        return node;
    }
    
    private static double distSquare(double [] x, double [] y) {
    	double distance = 0.0;
        for (int i = 0; i < x.length; ++i) {
        	distance += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return distance;
    }
    
    private void searchInRings(double[] q, Node node, double [] radiusSquares, long[] counts, double maxRadius) {
        if (node.isLeaf()) {
            // look at all the instances in this leaf
            for (int idx = node.index; idx < node.index + node.count; idx++) {
                double distance = distSquare(q, keys[index[idx]]);
                for (int i = radiusSquares.length - 1; i > 0; --i) {
                	if (distance < radiusSquares[i - 1]) {
                		continue;
                	}
                	if (distance < radiusSquares[i]) {
                		++counts[i - 1];
                	} 
                	return;
                }
                
            }
        } else {
            //Node nearer, further;
            double diff = q[node.split] - node.cutoff;
            if (diff < 0) {
                //nearer = node.lower;
                //further = node.upper;
            	searchInRings(q, node.lower, radiusSquares, counts, maxRadius);
                if (maxRadius >= -diff) {
                	 searchInRings(q, node.upper, radiusSquares, counts, maxRadius);
                }
            } else {
                //nearer = node.upper;
                //further = node.lower;
                searchInRings(q, node.upper, radiusSquares, counts, maxRadius);
                if (maxRadius >= diff) {
                    searchInRings(q, node.lower, radiusSquares, counts, maxRadius);
                }
            }

            
            //searchInRings(q, nearer, radiusSquares, counts, maxRadiusSqrt);
            // now look in further half
            /*if (maxRadiusSqrt >= diff) {
                searchInRings(q, further, radiusSquares, counts, maxRadiusSqrt);
            }*/
        }
    }
    
    public void countRanges(double[] q, double [] radiusSquares, long[] counts, double maxRadius) {
        searchInRings(q, root, radiusSquares, counts, maxRadius);
    }
    
}

