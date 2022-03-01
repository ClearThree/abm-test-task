"""
Алгоритм: два прохода. На первом находим индексы нулей в массиве,
на втором идем по индексам элементов и находим минимальное
расстояние до нуля слева или справа от текущего элемента.

Сложность по времени: O(n), так как пробегаем по всей последовательности.
Сложность по памяти: O(n), так как преобразуем входную строку в список значений, список храним в памяти.
Остальные списки (список индексов нулей) не может быть длиннее всего списка - поэтому O(n)

В случае отсутствия нулей в списке значений - возвращаем пустую строку.
"""


def distances_to_zeros(n: int, numbers: str) -> str:
    numbers = [int(number) for number in numbers.strip().split(" ")]
    if not numbers:
        return ""
    answer, zero_indices = [], []
    i = 0
    while i < n:
        if numbers[i] == 0:
            zero_indices.append(i)
        i += 1

    if not zero_indices:
        return ""
    if len(zero_indices) == 1:
        return " ".join([str(abs(i - zero_indices[0])) for i in range(n)])

    left_zero_index, right_zero_index = zero_indices[0], zero_indices[1]
    i, j = 0, 2
    while i < n:
        if abs(i - left_zero_index) < abs(right_zero_index - i):
            answer.append(str(abs(i - left_zero_index)))
        else:
            answer.append(str(abs(right_zero_index - i)))

        if i == right_zero_index and j < len(zero_indices):
            left_zero_index, right_zero_index = right_zero_index, zero_indices[j]
            j += 1
        i += 1
    return " ".join(answer)


print(distances_to_zeros(3, "0 0 0"))
