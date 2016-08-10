//
//  sort.hpp
//  realm-object-store
//
//  Created by Realm on 8/10/16.
//
//

#ifndef sort_hpp
#define sort_hpp

#include <vector>

namespace realm {
class SortDescriptor;
class Table;

class TransientSortDescriptor {
private:
    std::vector<std::vector<size_t>> m_column_indices;
    std::vector<bool> m_ascending;

public:
    TransientSortDescriptor(std::vector<std::vector<size_t>> column_indices, std::vector<bool> ascending={}) :
        m_column_indices(column_indices), m_ascending(ascending) {}

    SortDescriptor for_table(Table const& table);
};

};

#endif /* sort_hpp */
